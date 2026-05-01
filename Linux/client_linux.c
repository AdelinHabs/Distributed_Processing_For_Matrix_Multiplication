/*
 * client_linux.c  –  Linux version
 *
 * Compile:  gcc client_linux.c -o client
 *
 * Bug fixed: the previous version applied a 2-second SO_RCVTIMEO to
 * the coordinator connection socket.  The coordinator can take several
 * seconds to ping workers, distribute rows, and collect results — easily
 * longer than 2 s.  recv() would time out, return EAGAIN, recv_all()
 * would treat that as failure and return -1 without writing anything to
 * resp, leaving resp == 0, which printed "Unexpected response: 0".
 *
 * Fix: the coordinator connection socket has NO receive timeout.
 * Timeouts are only used for the short-lived discovery sockets
 * (heartbeat pings and UDP broadcast) where we genuinely need a fast
 * failure so we can try the next node.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define ANNOUNCE_PORT     7999
#define FILE_PATH         "nodes.txt"
#define MSG_TASK          1
#define MSG_RESULT        2
#define MSG_HEARTBEAT     5
#define MSG_HEARTBEAT_ACK 6

/* ── networking helpers ───────────────────────────────────────── */
static int send_all(int s, const char *buf, int len)
{
    int sent = 0, n;
    while (sent < len) {
        n = (int)send(s, buf + sent, (size_t)(len - sent), MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += n;
    }
    return sent;
}

static int recv_all(int s, char *buf, int len)
{
    int got = 0, n;
    while (got < len) {
        n = (int)recv(s, buf + got, (size_t)(len - got), 0);
        if (n <= 0) return -1;
        got += n;
    }
    return got;
}

/*
 * tcp_connect_timed  –  used for SHORT-LIVED discovery sockets only.
 * Applies a receive/send timeout so a dead node doesn't block us long.
 */
static int tcp_connect_timed(const char *ip, int port, int timeout_sec)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return -1;

    struct timeval tv = { timeout_sec, 0 };
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family      = AF_INET;
    a.sin_port        = htons((uint16_t)port);
    a.sin_addr.s_addr = inet_addr(ip);

    if (connect(s, (struct sockaddr*)&a, sizeof(a)) != 0) {
        close(s); return -1;
    }
    return s;
}

/*
 * tcp_connect_task  –  used for the coordinator task connection.
 * NO receive timeout: the coordinator may take many seconds to compute
 * results and we must wait as long as it needs.
 */
static int tcp_connect_task(const char *ip, int port)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return -1;

    /* only a send timeout — if we can't send data something is very wrong */
    struct timeval tv = { 10, 0 };
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    /* deliberately NO SO_RCVTIMEO */

    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family      = AF_INET;
    a.sin_port        = htons((uint16_t)port);
    a.sin_addr.s_addr = inet_addr(ip);

    if (connect(s, (struct sockaddr*)&a, sizeof(a)) != 0) {
        close(s); return -1;
    }
    return s;
}

/* ── method 1: UDP broadcast ──────────────────────────────────── */
static int try_udp_broadcast(char *out_ip, int *out_port)
{
    int us = socket(AF_INET, SOCK_DGRAM, 0);
    if (us < 0) return 0;

    int yes = 1;
    setsockopt(us, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(yes));

    struct timeval tv = { 3, 0 };
    setsockopt(us, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in bcast;
    memset(&bcast, 0, sizeof(bcast));
    bcast.sin_family      = AF_INET;
    bcast.sin_port        = htons(ANNOUNCE_PORT);
    bcast.sin_addr.s_addr = htonl(INADDR_BROADCAST);

    const char *q = "WHO_IS_COORDINATOR";
    sendto(us, q, (int)strlen(q), 0, (struct sockaddr*)&bcast, sizeof(bcast));

    char reply[64]; memset(reply, 0, sizeof(reply));
    struct sockaddr_in from; socklen_t fl = sizeof(from);
    int n = (int)recvfrom(us, reply, sizeof(reply)-1, 0,
                          (struct sockaddr*)&from, &fl);
    close(us);

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
 * Heartbeat every node in nodes.txt.
 * Highest responding port = coordinator (same rule nodes use).
 * Uses timed connections so dead nodes don't stall us.
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
        int s = tcp_connect_timed(ip, port, 2);
        if (s < 0) continue;

        int msg = MSG_HEARTBEAT;
        send_all(s, (char*)&msg, sizeof(int));
        int ack = 0;
        int ok = (recv_all(s, (char*)&ack, sizeof(int)) > 0
                  && ack == MSG_HEARTBEAT_ACK);
        close(s);

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

        printf("  No broadcast reply — scanning nodes.txt...\n");
        if (try_nodefile(out_ip, out_port)) {
            printf("Found coordinator via nodes.txt: %s:%d\n",
                   out_ip, *out_port);
            return;
        }

        printf("  No coordinator found yet. Retrying in 4 seconds...\n");
        sleep(4);
    }
}

/* ── main ─────────────────────────────────────────────────────── */
int main(void)
{
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
     * Connect to coordinator with NO receive timeout.
     * The coordinator may take several seconds to distribute work to
     * worker nodes and collect all results — we must wait it out.
     */
    int s = tcp_connect_task(coord_ip, coord_port);
    if (s < 0) {
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
        close(s); free(A); free(B); free(R);
        return 1;
    }

    if (resp == MSG_RESULT) {
        if (recv_all(s, (char*)R, sizeof(int) * rA * cB) <= 0) {
            printf("Connection closed while receiving result matrix.\n");
            close(s); free(A); free(B); free(R);
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

    close(s);
    free(A); free(B); free(R);
    return 0;
}
