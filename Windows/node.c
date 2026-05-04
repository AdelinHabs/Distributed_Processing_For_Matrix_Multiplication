/*
 * node.c  –  Windows version
 *
 * Compile:  gcc node.c -o node.exe -lws2_32
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <winsock2.h>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

/* ── tunables ─────────────────────────────────────────────────── */
#define FILE_PATH            "nodes.txt"
#define MAX_NODES            64
#define CONNECT_TIMEOUT_MS   2000   /* ms: TCP connect/recv timeout  */
#define HEALTH_INTERVAL_MS   8000   /* ms: between coordinator pings */
#define QUIET_AFTER_ELECTION 15     /* seconds: no new election after one */
#define ANNOUNCE_PORT        7999   /* UDP port: client finds coordinator */
/* ─────────────────────────────────────────────────────────────── */

#define MSG_TASK          1
#define MSG_RESULT        2
#define MSG_ELECTION      3
#define MSG_COORDINATOR   4
#define MSG_HEARTBEAT     5
#define MSG_HEARTBEAT_ACK 6
#define MSG_ELECTION_OK   7

typedef struct { char name[50]; char ip[32]; int port; } Node;

/* ── globals ──────────────────────────────────────────────────── */
static Node   g_nodes[MAX_NODES];
static int    g_node_count    = 0;
static int    g_is_coord      = 0;
static int    g_coord_port    = -1;
static char   g_coord_ip[32]  = "";
static int    g_election_busy = 0;
static int    g_task_busy     = 0;
static time_t g_last_election = 0;
static SOCKET g_server        = INVALID_SOCKET;
static int    g_my_port       = 0;
static char   g_my_ip[32]     = "";

/* ══════════════════════════════════════════════════════════════
 * IP DETECTION
 * Resolve hostname to find the machine's LAN IP.
 * Skips 127.x loopback. Falls back to 127.0.0.1.
 * ══════════════════════════════════════════════════════════════ */
static void detect_my_ip(void)
{
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        strcpy(g_my_ip, "127.0.0.1"); return;
    }

    struct hostent *he = gethostbyname(hostname);
    if (!he) { strcpy(g_my_ip, "127.0.0.1"); return; }

    for (int i = 0; he->h_addr_list[i] != NULL; i++) {
        struct in_addr addr;
        memcpy(&addr, he->h_addr_list[i], sizeof(addr));
        char *ip = inet_ntoa(addr);
        if (strncmp(ip, "127.", 4) == 0) continue;
        strncpy(g_my_ip, ip, sizeof(g_my_ip) - 1);
        return;
    }
    strcpy(g_my_ip, "127.0.0.1");
}

/* ══════════════════════════════════════════════════════════════
 * NETWORKING HELPERS
 * ══════════════════════════════════════════════════════════════ */
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

static SOCKET tcp_connect(const char *ip, int port)
{
    SOCKET s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == INVALID_SOCKET) return INVALID_SOCKET;

    /* Windows SO_RCVTIMEO/SO_SNDTIMEO takes milliseconds as a DWORD */
    DWORD t = CONNECT_TIMEOUT_MS;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&t, sizeof(t));
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (char*)&t, sizeof(t));

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

/* ══════════════════════════════════════════════════════════════
 * NODE REGISTRY
 * ══════════════════════════════════════════════════════════════ */
static void load_nodes(void)
{
    g_node_count = 0;
    FILE *f = fopen(FILE_PATH, "r");
    if (!f) return;
    char name[50], ip[32]; int port;
    while (fscanf(f, "%49s %31s %d", name, ip, &port) == 3
           && g_node_count < MAX_NODES)
    {
        int dup = 0;
        for (int i = 0; i < g_node_count; i++)
            if (g_nodes[i].port == port) { dup = 1; break; }
        if (!dup) {
            strcpy(g_nodes[g_node_count].name, name);
            strcpy(g_nodes[g_node_count].ip,   ip);
            g_nodes[g_node_count].port = port;
            g_node_count++;
        }
    }
    fclose(f);
}

static void register_self(const char *name, const char *ip, int port)
{
    FILE *f = fopen(FILE_PATH, "r");
    if (f) {
        char n[50], p_ip[32]; int p;
        while (fscanf(f, "%49s %31s %d", n, p_ip, &p) == 3)
            if (p == port) { fclose(f); return; }
        fclose(f);
    }
    f = fopen(FILE_PATH, "a");
    if (f) { fprintf(f, "%s %s %d\n", name, ip, port); fclose(f); }
}

/* ══════════════════════════════════════════════════════════════
 * LIVENESS
 * ══════════════════════════════════════════════════════════════ */
static int ping(const char *ip, int port)
{
    SOCKET s = tcp_connect(ip, port);
    if (s == INVALID_SOCKET) return 0;
    int msg = MSG_HEARTBEAT;
    send_all(s, (char*)&msg, sizeof(int));
    int ack = 0;
    int ok = (recv_all(s, (char*)&ack, sizeof(int)) > 0 && ack == MSG_HEARTBEAT_ACK);
    closesocket(s);
    return ok;
}

/* ══════════════════════════════════════════════════════════════
 * COORDINATOR DISCOVERY (startup)
 * ══════════════════════════════════════════════════════════════ */
static void discover_coordinator(void)
{
    load_nodes();
    int best = -1; char best_ip[32] = "";
    for (int i = 0; i < g_node_count; i++) {
        int p = g_nodes[i].port; const char *ip = g_nodes[i].ip;
        if (p == g_my_port) { if (p > best) { best = p; strcpy(best_ip, ip); } continue; }
        if (ping(ip, p) && p > best) { best = p; strcpy(best_ip, ip); }
    }
    if (best <= 0) best = g_my_port;
    g_coord_port = best;
    strcpy(g_coord_ip, (best == g_my_port) ? g_my_ip : best_ip);
    g_is_coord = (best == g_my_port);
}

/* ══════════════════════════════════════════════════════════════
 * ELECTION
 * ══════════════════════════════════════════════════════════════ */
static void broadcast_coordinator(void);

static void start_election(void)
{
    if (g_election_busy) return;
    if (time(NULL) - g_last_election < QUIET_AFTER_ELECTION) return;

    g_election_busy = 1;
    printf("[%d] Starting election...\n", g_my_port);

    /* Higher port = shorter delay = wins first */
    int delay_ms = (5 - (g_my_port % 5)) * 120;
    Sleep(delay_ms);

    /* Re-check: maybe another node already won during our sleep */
    if (time(NULL) - g_last_election < QUIET_AFTER_ELECTION) {
        printf("[%d] Election already settled. Aborting.\n", g_my_port);
        g_election_busy = 0;
        return;
    }

    load_nodes();
    int higher_alive = 0;
    for (int i = 0; i < g_node_count; i++) {
        int p = g_nodes[i].port;
        if (p <= g_my_port) continue;
        SOCKET s = tcp_connect(g_nodes[i].ip, p);
        if (s == INVALID_SOCKET) continue;
        int msg = MSG_ELECTION;
        send_all(s, (char*)&msg, sizeof(int));
        int resp = 0;
        if (recv_all(s, (char*)&resp, sizeof(int)) > 0 && resp == MSG_ELECTION_OK)
            higher_alive = 1;
        closesocket(s);
        if (higher_alive) break;
    }

    if (!higher_alive) {
        printf("[%d] I am the new coordinator.\n", g_my_port);
        g_is_coord   = 1;
        g_coord_port = g_my_port;
        strcpy(g_coord_ip, g_my_ip);
        g_last_election = time(NULL);
        broadcast_coordinator();   /* announce BEFORE clearing busy */
    }

    g_election_busy = 0;
    printf("[%d] Election done. Coordinator = port %d\n", g_my_port, g_coord_port);
}

static void broadcast_coordinator(void)
{
    load_nodes();
    for (int i = 0; i < g_node_count; i++) {
        if (g_nodes[i].port == g_my_port) continue;
        SOCKET s = tcp_connect(g_nodes[i].ip, g_nodes[i].port);
        if (s == INVALID_SOCKET) continue;
        int msg = MSG_COORDINATOR;
        send_all(s, (char*)&msg,       sizeof(int));
        send_all(s, (char*)&g_my_port, sizeof(int));
        send_all(s,  g_my_ip,          32);
        closesocket(s);
    }
}

/* ══════════════════════════════════════════════════════════════
 * UDP ANNOUNCE THREAD  (coordinator only)
 *
 * Listens on ANNOUNCE_PORT for "WHO_IS_COORDINATOR" broadcasts.
 * Replies with "ip port".
 * SO_REUSEADDR lets the new coordinator bind immediately after
 * the old one released the port.
 * ══════════════════════════════════════════════════════════════ */
static DWORD WINAPI announce_thread(LPVOID arg)
{
    (void)arg;

    SOCKET us = socket(AF_INET, SOCK_DGRAM, 0);
    if (us == INVALID_SOCKET) return 1;

    BOOL yes = TRUE;
    setsockopt(us, SOL_SOCKET, SO_REUSEADDR, (char*)&yes, sizeof(yes));

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = INADDR_ANY;
    sa.sin_port        = htons(ANNOUNCE_PORT);

    if (bind(us, (struct sockaddr*)&sa, sizeof(sa)) != 0) {
        printf("[%d] announce bind failed (error %d)\n",
               g_my_port, WSAGetLastError());
        closesocket(us); return 1;
    }

    printf("[%d] Announce thread ready on UDP port %d.\n",
           g_my_port, ANNOUNCE_PORT);

    char buf[64];
    struct sockaddr_in from;
    int fromlen;

    while (g_is_coord) {
        fromlen = sizeof(from);
        memset(buf, 0, sizeof(buf));
        int n = recvfrom(us, buf, sizeof(buf)-1, 0,
                         (struct sockaddr*)&from, &fromlen);
        if (n <= 0) continue;
        if (strcmp(buf, "WHO_IS_COORDINATOR") == 0) {
            char reply[64];
            snprintf(reply, sizeof(reply), "%s %d", g_my_ip, g_my_port);
            sendto(us, reply, (int)strlen(reply), 0,
                   (struct sockaddr*)&from, fromlen);
            printf("[%d] Answered coordinator query from %s.\n",
                   g_my_port, inet_ntoa(from.sin_addr));
        }
    }

    closesocket(us);
    return 0;
}

/* ══════════════════════════════════════════════════════════════
 * MATRIX COMPUTATION  (verbose step-by-step)
 * ══════════════════════════════════════════════════════════════ */
static int compute_row_col(const int *row, const int *col, int len, int ri, int ci)
{
    printf("[%d]   Cell R[%d][%d]:\n", g_my_port, ri, ci);
    int acc = 0;
    for (int k = 0; k < len; k++) {
        int prod = row[k] * col[k];
        printf("[%d]     multiplying %d x %d = %d\n",
               g_my_port, row[k], col[k], prod);
        if (k == 0) {
            acc = prod;
        } else {
            int prev = acc;
            acc += prod;
            printf("[%d]     adding %d + %d = %d\n",
                   g_my_port, prev, prod, acc);
        }
    }
    printf("[%d]     result = %d\n", g_my_port, acc);
    return acc;
}

static int get_live_workers(char out_ips[][32], int out_ports[])
{
    load_nodes();
    int cnt = 0;
    for (int i = 0; i < g_node_count; i++) {
        int p = g_nodes[i].port;
        if (p == g_my_port) continue;
        if (ping(g_nodes[i].ip, p)) {
            strcpy(out_ips[cnt], g_nodes[i].ip);
            out_ports[cnt] = p;
            cnt++;
        }
    }
    return cnt;
}

static void handle_task(SOCKET client)
{
    int rA, cA, rB, cB;
    if (recv_all(client, (char*)&rA, sizeof(int)) <= 0) return;
    if (recv_all(client, (char*)&cA, sizeof(int)) <= 0) return;
    if (recv_all(client, (char*)&rB, sizeof(int)) <= 0) return;
    if (recv_all(client, (char*)&cB, sizeof(int)) <= 0) return;
    if (cA != rB) { printf("Bad dimensions.\n"); return; }

    int *A = malloc(sizeof(int) * rA * cA);
    int *B = malloc(sizeof(int) * rB * cB);
    int *R = calloc(rA * cB, sizeof(int));
    if (!A || !B || !R) { free(A); free(B); free(R); return; }

    recv_all(client, (char*)A, sizeof(int) * rA * cA);
    recv_all(client, (char*)B, sizeof(int) * rB * cB);
    printf("[%d] Task: (%dx%d) x (%dx%d)\n", g_my_port, rA, cA, rB, cB);

    g_task_busy = 1;

    char w_ips[MAX_NODES][32]; int w_ports[MAX_NODES];
    int  w_cnt = get_live_workers(w_ips, w_ports);
    printf("[%d] %d worker(s) available.\n", g_my_port, w_cnt);

    if (w_cnt == 0) {
        printf("[%d] No workers — computing locally.\n", g_my_port);
        for (int i = 0; i < rA; i++)
            for (int j = 0; j < cB; j++) {
                int *col = malloc(sizeof(int) * rB);
                for (int k = 0; k < rB; k++) col[k] = B[k * cB + j];
                R[i * cB + j] = compute_row_col(&A[i * cA], col, cA, i, j);
                free(col);
            }
    } else {
        int w = 0;
        for (int i = 0; i < rA; i++) {
            int wi = w % w_cnt; w++;
            SOCKET ws = tcp_connect(w_ips[wi], w_ports[wi]);
            if (ws == INVALID_SOCKET) {
                printf("[%d] Worker %d down — row %d locally.\n",
                       g_my_port, w_ports[wi], i);
                for (int j = 0; j < cB; j++) {
                    int *col = malloc(sizeof(int) * rB);
                    for (int k = 0; k < rB; k++) col[k] = B[k * cB + j];
                    R[i * cB + j] = compute_row_col(&A[i * cA], col, cA, i, j);
                    free(col);
                }
                continue;
            }
            printf("[%d] Sending row %d to worker %d.\n",
                   g_my_port, i, w_ports[wi]);
            int msg = MSG_TASK;
            send_all(ws, (char*)&msg,       sizeof(int));
            send_all(ws, (char*)&cA,        sizeof(int));
            send_all(ws, (char*)&cB,        sizeof(int));
            send_all(ws, (char*)&A[i * cA], sizeof(int) * cA);
            send_all(ws, (char*)B,          sizeof(int) * rB * cB);
            recv_all(ws, (char*)&R[i * cB], sizeof(int) * cB);
            closesocket(ws);
            printf("[%d] Row %d done by worker %d.\n",
                   g_my_port, i, w_ports[wi]);
        }
    }

    g_task_busy = 0;
    int msg = MSG_RESULT;
    send_all(client, (char*)&msg, sizeof(int));
    send_all(client, (char*)R,    sizeof(int) * rA * cB);
    printf("[%d] Result sent to client.\n", g_my_port);
    free(A); free(B); free(R);
}

static void handle_worker_task(SOCKET client)
{
    int cA, cB;
    if (recv_all(client, (char*)&cA, sizeof(int)) <= 0) return;
    if (recv_all(client, (char*)&cB, sizeof(int)) <= 0) return;

    int *row = malloc(sizeof(int) * cA);
    int *B   = malloc(sizeof(int) * cA * cB);
    int *res = malloc(sizeof(int) * cB);
    if (!row || !B || !res) { free(row); free(B); free(res); return; }

    recv_all(client, (char*)row, sizeof(int) * cA);
    recv_all(client, (char*)B,   sizeof(int) * cA * cB);

    printf("[%d] Worker computing assigned row:\n", g_my_port);
    for (int j = 0; j < cB; j++) {
        int *col = malloc(sizeof(int) * cA);
        for (int k = 0; k < cA; k++) col[k] = B[k * cB + j];
        res[j] = compute_row_col(row, col, cA, 0, j);
        free(col);
    }

    send_all(client, (char*)res, sizeof(int) * cB);
    printf("[%d] Worker result sent.\n", g_my_port);
    free(row); free(B); free(res);
}

/* ══════════════════════════════════════════════════════════════
 * HEALTH THREAD  (worker nodes only)
 * ══════════════════════════════════════════════════════════════ */
static DWORD WINAPI health_thread(LPVOID arg)
{
    (void)arg;
    while (1) {
        Sleep(HEALTH_INTERVAL_MS);
        if (g_is_coord)        continue;
        if (g_task_busy)       continue;
        if (g_election_busy)   continue;
        if (g_coord_port <= 0) continue;
        if (time(NULL) - g_last_election < QUIET_AFTER_ELECTION) continue;

        if (!ping(g_coord_ip, g_coord_port)) {
            printf("[%d] Coordinator (port %d) not responding.\n",
                   g_my_port, g_coord_port);
            start_election();
        }
    }
    return 0;
}

/* ══════════════════════════════════════════════════════════════
 * MAIN ACCEPT LOOP
 * ══════════════════════════════════════════════════════════════ */
static void serve(void)
{
    while (1) {
        struct sockaddr_in ca; int cal = sizeof(ca);
        SOCKET client = accept(g_server, (struct sockaddr*)&ca, &cal);
        if (client == INVALID_SOCKET) { Sleep(5); continue; }

        int msg = 0;
        if (recv_all(client, (char*)&msg, sizeof(int)) <= 0) {
            closesocket(client); continue;
        }

        switch (msg) {
        case MSG_HEARTBEAT: {
            int ack = MSG_HEARTBEAT_ACK;
            send_all(client, (char*)&ack, sizeof(int));
            break;
        }
        case MSG_ELECTION: {
            int ok = MSG_ELECTION_OK;
            send_all(client, (char*)&ok, sizeof(int));
            break;
        }
        case MSG_COORDINATOR: {
            int new_port = 0; char new_ip[32] = "";
            recv_all(client, (char*)&new_port, sizeof(int));
            recv_all(client,  new_ip,           32);

            int was_coord   = g_is_coord;
            g_coord_port    = new_port;
            strcpy(g_coord_ip, new_ip);
            g_is_coord      = (new_port == g_my_port);
            g_election_busy = 0;
            g_last_election = time(NULL);
            printf("[%d] New coordinator: port %d\n", g_my_port, new_port);

            if (g_is_coord && !was_coord)
                CreateThread(NULL, 0, announce_thread, NULL, 0, NULL);
            break;
        }
        case MSG_TASK:
            if (g_is_coord) handle_task(client);
            else            handle_worker_task(client);
            break;
        default:
            printf("[%d] Unknown msg %d — ignored.\n", g_my_port, msg);
            break;
        }

        closesocket(client);
    }
}

/* ══════════════════════════════════════════════════════════════
 * MAIN
 * ══════════════════════════════════════════════════════════════ */
int main(void)
{
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);

    detect_my_ip();

    char self_name[50];
    printf("Enter node name (unique): ");
    fgets(self_name, sizeof(self_name), stdin);
    self_name[strcspn(self_name, "\n")] = 0;

    printf("Enter port     (unique): ");
    scanf("%d", &g_my_port); getchar();

    if (g_my_port <= 0 || g_my_port > 65535) { puts("Bad port."); return 1; }
    printf("Detected IP: %s\n", g_my_ip);

    g_server = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(g_server, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt));

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = INADDR_ANY;
    sa.sin_port        = htons((u_short)g_my_port);

    if (bind(g_server, (struct sockaddr*)&sa, sizeof(sa)) != 0) {
        printf("bind() failed — is port %d in use?\n", g_my_port); return 1;
    }
    listen(g_server, 10);
    printf("Node [%s]  IP=%s  port=%d\n", self_name, g_my_ip, g_my_port);

    register_self(self_name, g_my_ip, g_my_port);
    Sleep(300);
    discover_coordinator();

    if (g_is_coord) {
        printf("[%d] I am the coordinator.\n", g_my_port);
        g_last_election = time(NULL);
        broadcast_coordinator();
        CreateThread(NULL, 0, announce_thread, NULL, 0, NULL);
    } else {
        printf("[%d] Coordinator is port %d.\n", g_my_port, g_coord_port);
        CreateThread(NULL, 0, health_thread, NULL, 0, NULL);
    }

    serve();

    closesocket(g_server);
    WSACleanup();
    return 0;
}
