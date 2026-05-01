/*
 * node_linux.c  –  Linux version
 *
 * Compile:  gcc node_linux.c -o node -lpthread
 *
 * nodes.txt is created automatically.
 * Each node detects its own LAN IP on startup — no manual config.
 *
 * The coordinator answers UDP broadcasts on ANNOUNCE_PORT so
 * clients can find it without knowing any IP address.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ── tunables ─────────────────────────────────────────────────── */
#define FILE_PATH            "nodes.txt"
#define MAX_NODES            64
#define CONNECT_TIMEOUT      2      /* seconds: TCP connect/recv timeout  */
#define HEALTH_INTERVAL      8      /* seconds: between coordinator pings */
#define QUIET_AFTER_ELECTION 15     /* seconds: no new election after one */
#define ANNOUNCE_PORT        7999   /* UDP port: client finds coordinator */
/* ─────────────────────────────────────────────────────────────── */

#define INVALID_SOCKET (-1)
typedef int SOCKET;
static inline void closesocket(SOCKET s) { close(s); }
static inline void sleep_ms(int ms)      { usleep((useconds_t)ms * 1000); }

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
 * Walk all network interfaces, skip loopback and non-IPv4.
 * Use the first real LAN address found.
 * Falls back to 127.0.0.1 if nothing usable is found.
 * ══════════════════════════════════════════════════════════════ */
static void detect_my_ip(void)
{
    struct ifaddrs *ifap, *ifa;
    if (getifaddrs(&ifap) != 0) { strcpy(g_my_ip, "127.0.0.1"); return; }

    for (ifa = ifap; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) continue;
        struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;
        char *ip = inet_ntoa(sa->sin_addr);
        if (strncmp(ip, "127.", 4) == 0) continue;
        strncpy(g_my_ip, ip, sizeof(g_my_ip) - 1);
        break;
    }
    freeifaddrs(ifap);
    if (g_my_ip[0] == '\0') strcpy(g_my_ip, "127.0.0.1");
}

/* ══════════════════════════════════════════════════════════════
 * NETWORKING HELPERS
 * ══════════════════════════════════════════════════════════════ */
static int send_all(SOCKET s, const char *buf, int len)
{
    int sent = 0, n;
    while (sent < len) {
        n = (int)send(s, buf + sent, (size_t)(len - sent), MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += n;
    }
    return sent;
}

static int recv_all(SOCKET s, char *buf, int len)
{
    int got = 0, n;
    while (got < len) {
        n = (int)recv(s, buf + got, (size_t)(len - got), 0);
        if (n <= 0) return -1;
        got += n;
    }
    return got;
}

static SOCKET tcp_connect(const char *ip, int port)
{
    SOCKET s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == INVALID_SOCKET) return INVALID_SOCKET;

    struct timeval tv = { CONNECT_TIMEOUT, 0 };
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family      = AF_INET;
    a.sin_port        = htons((uint16_t)port);
    a.sin_addr.s_addr = inet_addr(ip);

    if (connect(s, (struct sockaddr*)&a, sizeof(a)) != 0) {
        close(s); return INVALID_SOCKET;
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
    close(s);
    return ok;
}

/* ══════════════════════════════════════════════════════════════
 * COORDINATOR DISCOVERY (startup)
 * Ping every node in the registry; highest port that responds = coordinator.
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
    sleep_ms(delay_ms);

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
        close(s);
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
        close(s);
    }
}

/* ══════════════════════════════════════════════════════════════
 * UDP ANNOUNCE THREAD  (coordinator only)
 *
 * Listens on ANNOUNCE_PORT for UDP broadcast "WHO_IS_COORDINATOR".
 * Replies with "ip port" so clients can find the coordinator
 * automatically.
 *
 * SO_REUSEADDR and SO_REUSEPORT are both set so the new coordinator
 * can bind the port immediately even if the old one just released it.
 * ══════════════════════════════════════════════════════════════ */
static void *announce_thread(void *arg)
{
    (void)arg;

    int us = socket(AF_INET, SOCK_DGRAM, 0);
    if (us < 0) return NULL;

    int yes = 1;
    setsockopt(us, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
    setsockopt(us, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = INADDR_ANY;
    sa.sin_port        = htons(ANNOUNCE_PORT);

    if (bind(us, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        perror("announce bind"); close(us); return NULL;
    }

    printf("[%d] Announce thread ready on UDP port %d.\n", g_my_port, ANNOUNCE_PORT);

    char buf[64];
    struct sockaddr_in from;
    socklen_t fromlen;

    while (g_is_coord) {
        fromlen = sizeof(from);
        memset(buf, 0, sizeof(buf));
        int n = (int)recvfrom(us, buf, sizeof(buf)-1, 0,
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

    close(us);
    return NULL;
}

/* ══════════════════════════════════════════════════════════════
 * MATRIX COMPUTATION  (verbose step-by-step)
 * ══════════════════════════════════════════════════════════════ */
static int compute_row_col(const int *row, const int *col, int len,
                            int ri, int ci)
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
            printf("[%d] Sending row %d to worker %d.\n", g_my_port, i, w_ports[wi]);
            int msg = MSG_TASK;
            send_all(ws, (char*)&msg,       sizeof(int));
            send_all(ws, (char*)&cA,        sizeof(int));
            send_all(ws, (char*)&cB,        sizeof(int));
            send_all(ws, (char*)&A[i * cA], sizeof(int) * cA);
            send_all(ws, (char*)B,          sizeof(int) * rB * cB);
            recv_all(ws, (char*)&R[i * cB], sizeof(int) * cB);
            close(ws);
            printf("[%d] Row %d done by worker %d.\n", g_my_port, i, w_ports[wi]);
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
static void *health_thread(void *arg)
{
    (void)arg;
    while (1) {
        sleep(HEALTH_INTERVAL);
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
    return NULL;
}

/* ══════════════════════════════════════════════════════════════
 * MAIN ACCEPT LOOP
 * ══════════════════════════════════════════════════════════════ */
static void serve(void)
{
    while (1) {
        struct sockaddr_in ca; socklen_t cal = sizeof(ca);
        SOCKET client = accept(g_server, (struct sockaddr*)&ca, &cal);
        if (client == INVALID_SOCKET) { sleep_ms(5); continue; }

        int msg = 0;
        if (recv_all(client, (char*)&msg, sizeof(int)) <= 0) {
            close(client); continue;
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

            if (g_is_coord && !was_coord) {
                pthread_t tid;
                pthread_create(&tid, NULL, announce_thread, NULL);
                pthread_detach(tid);
            }
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

        close(client);
    }
}

/* ══════════════════════════════════════════════════════════════
 * MAIN
 * ══════════════════════════════════════════════════════════════ */
int main(void)
{
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
    setsockopt(g_server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET; sa.sin_addr.s_addr = INADDR_ANY;
    sa.sin_port   = htons((uint16_t)g_my_port);

    if (bind(g_server, (struct sockaddr*)&sa, sizeof(sa)) != 0) {
        printf("bind() failed — is port %d in use?\n", g_my_port); return 1;
    }
    listen(g_server, 10);
    printf("Node [%s]  IP=%s  port=%d\n", self_name, g_my_ip, g_my_port);

    register_self(self_name, g_my_ip, g_my_port);
    sleep_ms(300);
    discover_coordinator();

    if (g_is_coord) {
        printf("[%d] I am the coordinator.\n", g_my_port);
        g_last_election = time(NULL);
        broadcast_coordinator();
        pthread_t tid;
        pthread_create(&tid, NULL, announce_thread, NULL);
        pthread_detach(tid);
    } else {
        printf("[%d] Coordinator is port %d.\n", g_my_port, g_coord_port);
        pthread_t tid;
        pthread_create(&tid, NULL, health_thread, NULL);
        pthread_detach(tid);
    }

    serve();
    close(g_server);
    return 0;
}
