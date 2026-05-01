/*
 * client.c  –  Windows version
 *
 * Compile:  gcc client.c -o client.exe -lws2_32
 *
 * Bug fixed: the previous version applied a 2000 ms SO_RCVTIMEO to
 * the coordinator connection socket.  The coordinator can take several
 * seconds to ping workers, distribute rows, and collect results — easily
 * longer than 2 s.  recv() would time out, recv_all() would return -1
 * without writing anything to resp, leaving resp == 0, which printed
 * "Unexpected response: 0".
 *
 * Fix: the coordinator connection socket has NO receive timeout.
 * Timeouts are only used for the short-lived discovery sockets
 * (heartbeat pings and UDP broadcast) where we need fast failure so
 * we can try the next node.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <winsock2.h>

#pragma comment(lib, "ws2_32.lib")

#define ANNOUNCE_PORT     7999
#define FILE_PATH         "nodes.txt"
#define MSG_TASK          1
#define MSG_RESULT        2
#define MSG_HEARTBEAT     5
#define MSG_HEARTBEAT_ACK 6

/* ── networking helpers ───────────────────────────────────────── */
static int send_all(SOCKET s, const char *buf, int len)
{
    int sent = 0, n;
    while (sent < len) {
        n = send(s, buf + sent, len - sent, 0);
        if (n <= 0) return -1;
        sent += n;
    }
    return sent;
}

static int recv_all(SOCKET s, char *buf, int len)
{
    int got = 0, n;
    while (got < len) {
        n = recv(s, buf + got, len - got, 0);
        if (n <= 0) return -1;
        got += n;
    }
    return got;
}

/*
 * tcp_connect_timed  –  SHORT-LIVED discovery sockets only.
 * Applies a timeout so dead nodes don't block discovery for long.
 */
static SOCKET tcp_connect_timed(const char *ip, int port, DWORD timeout_ms)
{
    SOCKET s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == INVALID_SOCKET) return INVALID_SOCKET;

    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout_ms, sizeof(timeout_ms));
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout_ms, sizeof(timeout_ms));

    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family      = AF_INET;
    a.sin_port        = htons((u_short)port);
    a.sin_addr.s_addr = inet_addr(ip);

    if (connect(s, (struct sockaddr*)&a, sizeof(a)) != 0) {
        closesocket(s); return INVALID_SOCKET;
    }
    return s;
}

/*
 * tcp_connect_task  –  coordinator task connection.
 * NO receive timeout: the coordinator may take many seconds to compute
 * and we must wait as long as it needs.
 */
static SOCKET tcp_connect_task(const char *ip, int port)
{
    SOCKET s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == INVALID_SOCKET) return INVALID_SOCKET;

    /* only a send timeout — deliberately NO SO_RCVTIMEO */
    DWORD send_t = 10000;
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (char*)&send_t, sizeof(send_t));

    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family      = AF_INET;
    a.sin_port        = htons((u_short)port);
    a.sin_addr.s_addr = inet_addr(ip);

    if (connect(s, (struct sockaddr*)&a, sizeof(a)) != 0) {
        closesocket(s); return INVALID_SOCKET;
    }
    return s;
}

/* ── method 1: UDP broadcast ──────────────────────────────────── */
static int try_udp_broadcast(char *out_ip, int *out_port)
{
    SOCKET us = socket(AF_INET, SOCK_DGRAM, 0);
    if (us == INVALID_SOCKET) return 0;

    BOOL yes = TRUE;
    setsockopt(us, SOL_SOCKET, SO_BROADCAST, (char*)&yes, sizeof(yes));

    DWORD t = 3000;
    setsockopt(us, SOL_SOCKET, SO_RCVTIMEO, (char*)&t, sizeof(t));

    struct sockaddr_in bcast;
    memset(&bcast, 0, sizeof(bcast));
    bcast.sin_family      = AF_INET;
    bcast.sin_port        = htons(ANNOUNCE_PORT);
    bcast.sin_addr.s_addr = INADDR_BROADCAST;

    const char *q = "WHO_IS_COORDINATOR";
    sendto(us, q, (int)strlen(q), 0, (struct sockaddr*)&bcast, sizeof(bcast));

    char reply[64]; memset(reply, 0, sizeof(reply));
    struct sockaddr_in from; int fromlen = sizeof(from);
    int n = recvfrom(us, reply, sizeof(reply)-1, 0,
                     (struct sockaddr*)&from, &fromlen);
    closesocket(us);

    if (n > 0) {
        char ip[32]; int port;
        if (sscanf(reply, "%31s %d", ip, &port) == 2) {
            strcpy(out_ip, ip);
            *out_port = port;
            return 1;
        }
    }
    return 0;
}

/* ── method 2: scan nodes.txt ─────────────────────────────────── *
 * Heartbeat every node in nodes.txt with a short timeout.
 * Highest responding port = coordinator.
 */
static int try_nodefile(char *out_ip, int *out_port)
{
    FILE *f = fopen(FILE_PATH, "r");
    if (!f) {
        printf("  (nodes.txt not found — only UDP broadcast available)\n");
        return 0;
    }

    char name[50], ip[32]; int port;
    int  best = -1; char best_ip[32] = "";

    while (fscanf(f, "%49s %31s %d", name, ip, &port) == 3) {
        SOCKET s = tcp_connect_timed(ip, port, 2000);
        if (s == INVALID_SOCKET) continue;

        int msg = MSG_HEARTBEAT;
        send_all(s, (char*)&msg, sizeof(int));
        int ack = 0;
        int ok = (recv_all(s, (char*)&ack, sizeof(int)) > 0
                  && ack == MSG_HEARTBEAT_ACK);
        closesocket(s);

        if (ok && port > best) {
            best = port;
            strcpy(best_ip, ip);
        }
    }
    fclose(f);

    if (best > 0) {
        strcpy(out_ip, best_ip);
        *out_port = best;
        return 1;
    }
    return 0;
}

/* ── find coordinator (retries forever) ───────────────────────── */
static void find_coordinator(char *out_ip, int *out_port)
{
    int attempt = 0;
    while (1) {
        attempt++;
        printf("Searching for coordinator (attempt %d)...\n", attempt);

        if (try_udp_broadcast(out_ip, out_port)) {
            printf("Found coordinator via broadcast: %s:%d\n",
                   out_ip, *out_port);
            return;
        }

        printf("  No broadcast reply -> scanning nodes.txt...\n");
        if (try_nodefile(out_ip, out_port)) {
            printf("Found coordinator via nodes.txt: %s:%d\n",
                   out_ip, *out_port);
            return;
        }

        printf("  No coordinator found yet. Retrying in 4 seconds...\n");
        Sleep(4000);
    }
}

/* ── main ─────────────────────────────────────────────────────── */
int main(void)
{
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);

    char coord_ip[32]; int coord_port;
    find_coordinator(coord_ip, &coord_port);

    int rA, cA, rB, cB;
    printf("\nWe compute C = A x B  (columns of A must equal rows of B)\n\n");
    while (1) {
        printf("Rows    of A: "); scanf("%d", &rA);
        printf("Columns of A: "); scanf("%d", &cA);
        printf("Rows    of B: "); scanf("%d", &rB);
        printf("Columns of B: "); scanf("%d", &cB);
        if (cA == rB) break;
        printf("ERROR: columns of A (%d) must equal rows of B (%d).\n\n", cA, rB);
    }

    int *A = malloc(sizeof(int) * rA * cA);
    int *B = malloc(sizeof(int) * rB * cB);
    int *R = malloc(sizeof(int) * rA * cB);
    if (!A || !B || !R) { fprintf(stderr, "Out of memory.\n"); return 1; }

    printf("\nEnter Matrix A (%d x %d):\n", rA, cA);
    for (int i = 0; i < rA * cA; i++) scanf("%d", &A[i]);

    printf("\nEnter Matrix B (%d x %d):\n", rB, cB);
    for (int i = 0; i < rB * cB; i++) scanf("%d", &B[i]);

    /*
     * Connect with NO receive timeout.
     * The coordinator may take many seconds to distribute work across
     * worker nodes and collect results — we must wait it out.
     */
    SOCKET s = tcp_connect_task(coord_ip, coord_port);
    if (s == INVALID_SOCKET) {
        printf("Could not connect to coordinator at %s:%d\n",
               coord_ip, coord_port);
        free(A); free(B); free(R);
        return 1;
    }
    printf("\nConnected to coordinator at %s:%d\n", coord_ip, coord_port);

    int msg = MSG_TASK;
    send_all(s, (char*)&msg, sizeof(int));
    send_all(s, (char*)&rA,  sizeof(int));
    send_all(s, (char*)&cA,  sizeof(int));
    send_all(s, (char*)&rB,  sizeof(int));
    send_all(s, (char*)&cB,  sizeof(int));
    send_all(s, (char*)A,    sizeof(int) * rA * cA);
    send_all(s, (char*)B,    sizeof(int) * rB * cB);
    printf("Matrices sent. Waiting for result...\n");

    int resp = 0;
    if (recv_all(s, (char*)&resp, sizeof(int)) <= 0) {
        printf("Connection closed before result arrived.\n");
        closesocket(s); free(A); free(B); free(R);
        return 1;
    }

    if (resp == MSG_RESULT) {
        if (recv_all(s, (char*)R, sizeof(int) * rA * cB) <= 0) {
            printf("Connection closed while receiving result matrix.\n");
            closesocket(s); free(A); free(B); free(R);
            return 1;
        }
        printf("\nResult matrix C (%d x %d):\n", rA, cB);
        for (int i = 0; i < rA; i++) {
            for (int j = 0; j < cB; j++)
                printf("%6d ", R[i * cB + j]);
            printf("\n");
        }
    } else {
        printf("Unexpected response type: %d\n", resp);
    }

    closesocket(s);
    WSACleanup();
    free(A); free(B); free(R);
    return 0;
}
