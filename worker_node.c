#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <iphlpapi.h>
#include <time.h>


// Coordinator Address
char coordinator_ip_address[32] =  "127.0.0.1";
int coordinator_port = 6000;

// char coordinator_ip_address[32];
// int coordinator_port;

// Coordinator status
int is_coordinator = 0;
int coordinator_known = 0;

// Election flag
int election_in_progress = 0;

// MESSAGE TYPES
#define MSG_TASK               1
#define MSG_RESULT             2
#define MSG_ELECTION           3
#define MSG_COORDINATOR        4
#define MSG_HEARTBEAT          5
#define MSG_HEARTBEAT_ACK      6
#define MSG_NODE_SELF_REGISTER 7
#define MSG_BROADCAST_NEW_NODE 8
#define MSG_REQUEST_NODE_LIST  9
#define MSG_UPDATE_NODE_LIST   10
#define MSG_ELECTION_OK        11

// Arrays to store available worker nodes
#define MAX_NODES 100

char worker_names[MAX_NODES][50];
char worker_ips[MAX_NODES][32];
int worker_ports[MAX_NODES];

typedef struct {
    int type;
} MessageHeader;

typedef struct {
    int type;
    char name[50];
    char ip[32];
    int port;
} NodeMessage;

typedef struct {
    char name[50];
    char ip[32];
    int port;
} Node;

Node nodes[MAX_NODES];
int node_count = 0;

// For networking
SOCKET server_socket;
struct sockaddr_in address;
int addrlen = sizeof(address);


// ------------- Linking the Winsock networking library -------------
#pragma comment(lib, "ws2_32.lib")
// ------------- Linking the windows API to get the IP Address ------------
// #pragma comment(lib, "iphlpapi.lib")
// #define WIN32_LEAN_AND_MEAN


// ================================== METHOD DECLARATIONS ======================================
void get_string(char *prompt, char *buffer, int size);
int get_int(char *prompt);
int send_all(SOCKET sock, char *buffer, int length);
int recv_all(SOCKET sock, char *buffer, int length);
SOCKET connect_to_node(const char *ip, int port);
void node_self_register(char *self_name, char *self_ip, int my_port);
void merge_node(char *name, char *ip, int port);
void broadcast_new_node(Node new_node, char *self_name);
void update_node_list(char *data);
void request_node_list();
void send_node_list(SOCKET client);
int get_available_workers();
void remove_dead_nodes();
void handle_task(SOCKET client);
void append_node_to_file(Node new_node, char *self_name);
void broadcast_coordinator(int my_port);
int ping_coordinator();
int discover_coordinator(char *my_ip, int my_port);
void start_election(int my_port);
void run_as_coordinator(int my_port);
void run_as_worker(char *my_ip, int my_port);




// ======================================== Helper methods =======================================================================
void get_string(char *prompt, char *buffer, int size) {
    printf("%s\n", prompt);
    fgets(buffer, size, stdin);
    buffer[strcspn(buffer, "\n")] = 0;
}

int get_int(char *prompt) {
    char temp[10];
    printf("%s\n", prompt);
    fgets(temp, sizeof(temp), stdin);
    return atoi(temp);
}
// ======================================== Helper methods =======================================================================


// send_all
int send_all(SOCKET sock, char *buffer, int length) {
    printf("\nIn send all\n");

    int total = 0, bytes;

    while (total < length) {
        bytes = send(sock, buffer + total, length - total, 0);
        if (bytes <= 0) return -1;
        total += bytes;
    }
    return total;
}

// recv_all
int recv_all(SOCKET sock, char *buffer, int length) {
    printf("\nIn recv all\n");

    int total = 0, bytes;

    while (total < length) {
        bytes = recv(sock, buffer + total, length - total, 0);
        if (bytes <= 0) return -1;
        total += bytes;
    }
    return total;
}

// Connect to Node
SOCKET connect_to_node(const char *ip, int port) {
    printf("\nIn connect Node\n");

    SOCKET sock;
    struct sockaddr_in node;

    // Create socket
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        printf("Socket creation failed\n");
        return INVALID_SOCKET;
    }

    // Setup node structure
    node.sin_family = AF_INET;
    node.sin_port = htons(port);
    node.sin_addr.s_addr = inet_addr(ip);

    // Connect to node
    if (connect(sock, (struct sockaddr*)&node, sizeof(node)) < 0) {
        printf("Connection failed to %s:%d\n", ip, port);
        closesocket(sock);
        return INVALID_SOCKET;
    }

    return sock;
}

// Node self register
void node_self_register(char *self_name, char *self_ip, int my_port) {
    printf("\nIn Node self register\n");

    SOCKET sock = connect_to_node(coordinator_ip_address, coordinator_port);
    if (sock == INVALID_SOCKET) return;

    char message[256];
    sprintf(message, "%d %s %s %d",
            MSG_NODE_SELF_REGISTER,
            self_name,
            self_ip,
            my_port);

    send_all(sock, message, strlen(message));
    closesocket(sock);
}

// Update_local_memory
void merge_node(char *name, char *ip, int port) {
    printf("\nIn Merge Node\n");

    if (node_count >= MAX_NODES) return;

    for (int i = 0; i < node_count; i++) {
        if (strcmp(nodes[i].name, name) == 0)
            return; // already exists
    }

    strcpy(nodes[node_count].name, name);
    strcpy(nodes[node_count].ip, ip);
    nodes[node_count].port = port;

    node_count++;
}

// Load nodes from file
void load_nodes_from_file() {
    printf("\nIn Load Nodes from file\n");

    FILE *file = fopen("c:\\Users\\HP\\Desktop\\nodes.txt", "r");
    if (!file) return;

    char name[50], ip[50];
    int port;

    while (fscanf(file, "%s %s %d", name, ip, &port) == 3) {
        merge_node(name, ip, port);
    }

    fclose(file);
}

// Append Node to file
void append_node_to_file(Node new_node, char *self_name){
    printf("\nIn Append Node to file\n");

    FILE *file = fopen("c:\\Users\\HP\\Desktop\\nodes.txt", "a");
    if (file != NULL) {
        fprintf(file, "%s %s %d\n",
                new_node.name,
                new_node.ip,
                new_node.port);
        fclose(file);
    }
}

// Broadcast new node to all other nodes by coordinator and update registry
void broadcast_new_node(Node new_node, char *self_name) {
    printf("\nIn Broadcast new Node\n");

    FILE *file;
    char name[50], ip[50];
    int port;

    char message[256];

    // 1. Format message
    sprintf(message, "%d %s %s %d",
            MSG_BROADCAST_NEW_NODE,
            new_node.name,
            new_node.ip,
            new_node.port);

    // 2. Add to local memory (self update)
    merge_node(new_node.name, new_node.ip, new_node.port);

    // 3. Append to file (only if not already there ideally)
    append_node_to_file(new_node, self_name);

    // 4. Broadcast to OTHER nodes
    file = fopen("c:\\Users\\HP\\Desktop\\nodes.txt", "r");
    if (file == NULL) return;

    while (fscanf(file, "%s %s %d", name, ip, &port) == 3) {

        // skip self
        if (strcmp(name, self_name) == 0)
            continue;

        SOCKET sock = connect_to_node(ip, port);

        if (sock != INVALID_SOCKET) {
            send_all(sock, message, strlen(message));
            closesocket(sock);
        }
    }

    fclose(file);
}

// update Node list
void update_node_list(char *data) {
    printf("\nUpdate Node list\n");

    char *token = strtok(data, " ");

    if (!token) return;
    if (atoi(token) != MSG_UPDATE_NODE_LIST) return;

    while ((token = strtok(NULL, " ")) != NULL) {

        char name[50], ip[50];
        int port;

        strcpy(name, token);

        token = strtok(NULL, " ");
        if (!token) break;
        strcpy(ip, token);

        token = strtok(NULL, " ");
        if (!token) break;
        port = atoi(token);

        merge_node(name, ip, port);
    }
}

// Request node list 
void request_node_list() {
    printf("\nRequest Node list\n");

    FILE *file;
    char name[50], ip[50];
    int port;

    char message[64];
    sprintf(message, "%d", MSG_REQUEST_NODE_LIST);

    char buffer[2048];

    file = fopen("c:\\Users\\HP\\Desktop\\nodes.txt", "r");
    if (file == NULL) return;

    while (fscanf(file, "%s %s %d", name, ip, &port) == 3) {

        SOCKET sock = connect_to_node(ip, port);
        if (sock == INVALID_SOCKET) continue;

        // 1. Send request
        send_all(sock, message, strlen(message));

        // 2. Receive response
        int bytes = recv_all(sock, buffer, sizeof(buffer) - 1);

        if (bytes > 0) {
            buffer[bytes] = '\0';

            // 3. Merge received list
            update_node_list(buffer);
        }

        closesocket(sock);
    }

    fclose(file);
}

// Coordinator sends node list
void send_node_list(SOCKET client) {
    printf("\nSend Node list\n");

    char buffer[2048];
    int offset = 0;

    offset += sprintf(buffer + offset, "%d ", MSG_UPDATE_NODE_LIST);

    for (int i = 0; i < node_count; i++) {
        offset += sprintf(buffer + offset, "%s %s %d ",
                          nodes[i].name,
                          nodes[i].ip,
                          nodes[i].port);
    }

    send_all(client, buffer, strlen(buffer));
}

// Get available workers
int get_available_workers() {
    printf("\nGet available workers\n");

    FILE *file = fopen("c:\\Users\\HP\\Desktop\\nodes.txt", "r");

    if (file == NULL) {
        printf("Error opening file\n");
        return 0;
    }

    char name[50], ip[32];
    int port;
    int counter = 0;

    while (fscanf(file, "%50s %32s %d", name, ip, &port) == 3) {

        strcpy(worker_names[counter], name);
        strcpy(worker_ips[counter], ip);
        worker_ports[counter] = port;

        counter++;
    }

    fclose(file);

    return counter;
}

// Detect and remove dead nodes
void remove_dead_nodes() {
    printf("\nIn Remove Dead Nodes\n");

    for (int i = 0; i < node_count; i++) {

        SOCKET s = connect_to_node(nodes[i].ip, nodes[i].port);

        if (s == INVALID_SOCKET) {
            printf("Node %s is dead. Removing...\n", nodes[i].name);

            // shift array left
            for (int j = i; j < node_count - 1; j++) {
                nodes[j] = nodes[j + 1];
            }

            node_count--;
            i--;
            continue;
        }

        int msg = MSG_HEARTBEAT;
        send_all(s, (char*)&msg, sizeof(int));

        int ack;
        if (recv_all(s, (char*)&ack, sizeof(int)) <= 0) {
            printf("Node %s not responding. Removing...\n", nodes[i].name);

            for (int j = i; j < node_count - 1; j++) {
                nodes[j] = nodes[j + 1];
            }

            node_count--;
            i--;
        }

        closesocket(s);
    }
}

// Handle tasks
void handle_task(SOCKET client) {
    printf("\nIn Handle Task\n");

    // int rowsA, colsA, rowsB, colsB;

    typedef struct {
        int rows_of_A;
        int cols_of_A;
        int rows_of_B;
        int cols_of_B;
    } MessageHeader;

    MessageHeader header;

    // Receive matrix sizes
    printf("\nReceiving Header A\n");
    recv_all(client, (char*)&header, sizeof(header));

    int rowsA = header.rows_of_A;
    int colsA = header.cols_of_A;
    int rowsB = header.rows_of_B;
    int colsB = header.cols_of_B;

    // recv_all(client, (char*)&colsA, sizeof(int));
    // recv_all(client, (char*)&rowsB, sizeof(int));
    // recv_all(client, (char*)&colsB, sizeof(int));

    // Validate multiplication condition
    if (colsA != rowsB) {
        printf("Invalid matrix dimensions\n");
        return;
    }

    // Receive matrices
    int A[rowsA][colsA], B[rowsB][colsB];

    recv_all(client, (char*)A, sizeof(int) * rowsA * colsA);
    recv_all(client, (char*)B, sizeof(int) * rowsB * colsB);

    // Load workers
    int worker_count = get_available_workers();
    
    if (worker_count == 0) {
        printf("No workers available\n");
        return;
    }

    int result[rowsA][colsB];

    int worker_index = 0;

    // Distribute tasks (each cell)
    for (int i = 0; i < rowsA; i++) {
        for (int j = 0; j < colsB; j++) {

            // Pick worker (round-robin)
            int w = worker_index % worker_count;
            worker_index++;

            // Create socket for worker
            SOCKET worker_sock = socket(AF_INET, SOCK_STREAM, 0);

            struct sockaddr_in worker_addr;
            worker_addr.sin_family = AF_INET;
            worker_addr.sin_port = htons(worker_ports[w]);
            worker_addr.sin_addr.s_addr = inet_addr(worker_ips[w]);

            connect(worker_sock, (struct sockaddr*)&worker_addr, sizeof(worker_addr));

            // Send task
            int msg = MSG_TASK;
            send_all(worker_sock, (char*)&msg, sizeof(int));

            // send size of vector
            send_all(worker_sock, (char*)&colsA, sizeof(int));

            // send row i
            send_all(worker_sock, (char*)A[i], sizeof(int) * colsA);

            // send column j
            int column[rowsB];
            for (int k = 0; k < rowsB; k++) {
                column[k] = B[k][j];
            }
            send_all(worker_sock, (char*)column, sizeof(int) * rowsB);

            // Receive result
            int cell_result;
            recv_all(worker_sock, (char*)&cell_result, sizeof(int));

            result[i][j] = cell_result;

            closesocket(worker_sock);
        }
    }

    // Send final result back to client
    int msg = MSG_RESULT;
    send_all(client, (char*)&msg, sizeof(int));
    send_all(client, (char*)result, sizeof(int) * rowsA * colsB);

    printf("Task completed and sent to client\n");
}

// Broadcast Coordinator
void broadcast_coordinator(int my_port) {
    printf("\nIn Broadcast coordinator\n");
    
    FILE *file = fopen("c:\\Users\\HP\\Desktop\\nodes.txt", "r");
    if (!file) return;

    char name[50], ip[32];
    int port;

    while (fscanf(file, "%s %s %d", name, ip, &port) == 3) {
        if (port != my_port) {

            SOCKET s = socket(AF_INET, SOCK_STREAM, 0);

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = inet_addr(ip);

            if (connect(s, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
                int msg = MSG_COORDINATOR;
                send_all(s, (char*)&msg, sizeof(int));
                send_all(s, (char*)&my_port, sizeof(int));
            }

            closesocket(s);
        }
    }

    fclose(file);
}

// Check if the coordinator is online
int ping_coordinator() {
    printf("\nIn Ping Coordinator\n");

    SOCKET s = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in coord_addr;
    coord_addr.sin_family = AF_INET;
    coord_addr.sin_port = htons(coordinator_port);
    coord_addr.sin_addr.s_addr = inet_addr(coordinator_ip_address);

    if (connect(s, (struct sockaddr*)&coord_addr, sizeof(coord_addr)) != 0)
        return 0;

    int msg = MSG_HEARTBEAT;
    send_all(s, (char*)&msg, sizeof(int));

    int ack;
    if (recv_all(s, (char*)&ack, sizeof(int)) <= 0)
        return 0;

    closesocket(s);
    return 1;
}
int discover_coordinator(char *my_ip, int my_port) {
    printf("\nIn discover Coordinator\n");

    FILE *file = fopen("c:\\Users\\HP\\Desktop\\nodes.txt", "r");
    if (!file) return 0;

    char name[50], ip[32];
    int port;

    int max_port = -1;
    char ip_of_max_port[32];

    int result;

    // =============================================================
    int count = 0;
    while (fscanf(file, "%49s %49s %d", name, ip, &port) == 3) {
        count++;
    }
    
    rewind(file);

    for (int i = 0; i < count; i++) {

        if (fscanf(file, "%49s %49s %d", name, ip, &port) != 3) {
            break;  // safety check
        }

         SOCKET s = connect_to_node(ip, port);

        if (port == my_port) {
            if (port > max_port) {
                max_port = my_port;
                strcpy(ip_of_max_port, my_ip);
            }
            
            
            printf("\n 1 ===================================================\n");
            continue;

        } 

        if (s == INVALID_SOCKET) continue;

        int msg = MSG_HEARTBEAT;
        send_all(s, (char*)&msg, sizeof(int));

        int ack;

        int timeout = 2000; // 2 seconds
        setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
        
        if (recv_all(s, (char*)&ack, sizeof(int)) > 0 && ack == MSG_HEARTBEAT_ACK) {

            if (port > max_port) {
                max_port = port;
                strcpy(ip_of_max_port, ip);
            }
        

            closesocket(s);
            printf("\n 2 ===================================================\n");
        }
    }

    fclose(file);

    // ==============================================================



    if (max_port == -1) {
        coordinator_known = 0;
        return 0; // no coordinator found
    }

    if (my_port == max_port) {
        is_coordinator = 1;
        printf("\n 3 ===================================================\n");
    } else {
        is_coordinator = 0;
        printf("\n 4 ===================================================\n");
    }

    coordinator_known = 1;
    coordinator_port = max_port;
    strcpy(coordinator_ip_address, ip_of_max_port);

    printf("\n 5 ===================================================\n");

    return 1;
}


// Election
void start_election(int my_port) {
    printf("[NODE %d] ENTER start_election | is_coordinator=%d election=%d\n",
           my_port, is_coordinator, election_in_progress);


    if (election_in_progress) return;
    election_in_progress = 1;

    // RANDOM BACKOFF (CRITICAL FIX)
    Sleep((rand() % 1000) + 500);

    int higher_found = 0;

    FILE *file = fopen("c:\\Users\\HP\\Desktop\\nodes.txt", "r");
    if (!file) return;

    char name[50], ip[32];
    int port;

    while (fscanf(file, "%s %s %d", name, ip, &port) == 3) {
        if (port > my_port) {
            SOCKET s = socket(AF_INET, SOCK_STREAM, 0);

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = inet_addr(ip);

            if (connect(s, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
                int msg = MSG_ELECTION;
                if(send_all(s, (char*)&msg, sizeof(int))){
                    // Wait for response
                    int response;

                    int timeout = 3000; // 3 seconds
                    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
                    
                    if (recv_all(s, (char*)&response, sizeof(int)) > 0) {

                        if (response == MSG_ELECTION_OK) {
                            higher_found = 1;
                            closesocket(s);
                            break; 
                        }
                    }
                }
            }

            closesocket(s);
        }
    }

    fclose(file);

    if (higher_found == 0 && is_coordinator == 0) {
        printf("\nI am the new coordinator\n");
        
        Sleep(1000);
        
        is_coordinator = 1;
        coordinator_known = 1;

        printf("[NODE %d] STATE UPDATED -> COORDINATOR\n", my_port);

        request_node_list();
        broadcast_coordinator(my_port);
    }

    election_in_progress = 0;

    printf("[NODE %d] EXIT start_election | is_coordinator=%d election=%d\n",
       my_port, is_coordinator, election_in_progress);
}

// If Coordinator Run this
void run_as_coordinator(int my_port) {
    while (1) {
        printf("[NODE %d] COORDINATOR LOOP RUNNING\n", my_port);

        // remove_dead_nodes();

        SOCKET client = accept(server_socket, (struct sockaddr*)&address, &addrlen);
        printf("[NODE %d] ACCEPTING CONNECTION...\n", my_port);

        int msg_type;

        printf("Connection accepted\n");
        int n = recv_all(client, (char*)&msg_type, sizeof(int));
        printf("Bytes received: %d\n", n);

        // printf("Waiting for message...\n");
        // if (recv_all(client, (char*)&msg_type, sizeof(int)) <= 0) {
        //     closesocket(client);
        //     continue;
        // }
        printf("Received bytes\n");
        if (msg_type == MSG_TASK) {
            printf("Called handle_task\n");
            handle_task(client);
            printf("Done handle_task\n");
        } else if (msg_type == MSG_ELECTION) {
            // reply OK and start your own election
            int msg = MSG_ELECTION_OK;
            send_all(client, (char*)&msg, sizeof(int));

            start_election(my_port);
            
        } else if (msg_type == MSG_HEARTBEAT) {
            int ack = MSG_HEARTBEAT_ACK;
            send_all(client, (char*)&ack, sizeof(int));
        } else if (msg_type == MSG_NODE_SELF_REGISTER) {

            char buffer[256];
            int bytes = recv_all(client, buffer, sizeof(buffer)-1);
            if (bytes <= 0) {
                closesocket(client);
                continue;
            }

            buffer[bytes] = '\0';

            char name[50], ip[50];
            int port;

            sscanf(buffer, "%s %s %d", name, ip, &port);

            Node new_node;
            strcpy(new_node.name, name);
            strcpy(new_node.ip, ip);
            new_node.port = port;

            printf("New node registered: %s %s %d\n", name, ip, port);

            broadcast_new_node(new_node, name);
        } else if (msg_type == MSG_REQUEST_NODE_LIST) {
            send_node_list(client);
        }
        
        closesocket(client);
        printf("\nClosed the socket\n");
    }
}

// If worker Run this
void run_as_worker(char *my_ip, int my_port) {
    while (!is_coordinator) {
        printf("[NODE %d] WORKER LOOP | is_coordinator=%d\n",
                my_port, is_coordinator);

        // Check coordinator periodically
        if (!discover_coordinator(my_ip, my_port)) {
            Sleep(1000);
            printf("Coordinator dead. Starting election...\n");
            // start_election(my_port);

            broadcast_coordinator(my_port);
            is_coordinator = 1;
            coordinator_known = 1;

            run_as_coordinator(my_port);

            continue;
        }

        SOCKET client = accept(server_socket, (struct sockaddr*)&address, &addrlen);
        printf("[NODE %d] ACCEPTING CONNECTION...\n", my_port);
        
        if (client == INVALID_SOCKET) continue;

        int msg_type;
        if (recv_all(client, (char*)&msg_type, sizeof(int)) <= 0) {
            closesocket(client);
            continue;

        }

        if (msg_type == MSG_TASK) {
            int size;

            // Receive vector size
            recv_all(client, (char*)&size, sizeof(int));

            // int row[100], column[100];
            int *row = malloc(sizeof(int) * size);
            int *column = malloc(sizeof(int) * size);

            // Receive row
            recv_all(client, (char*)row, sizeof(int) * size);

            // Receive column
            recv_all(client, (char*)column, sizeof(int) * size);

            // 4. Compute dot product
            int result = 0;
            for (int i = 0; i < size; i++) {
                result += row[i] * column[i];
            }

            // Send result back
            send_all(client, (char*)&result, sizeof(int));

            // Free Memory
            free(row);
            free(column);
        } else if (msg_type == MSG_ELECTION) {
            int msg = MSG_ELECTION_OK;
            send_all(client, (char*)&msg, sizeof(int));

            start_election(my_port);
        } else if (msg_type == MSG_COORDINATOR) {
            int new_coordinator_port;

            // Receive coordinator info
            recv_all(client, (char*)&new_coordinator_port, sizeof(int));

            coordinator_port = new_coordinator_port;
            coordinator_known = 1;
            is_coordinator = 0;

            printf("New coordinator is on port %d\n", coordinator_port);
            printf("[NODE %d] RECEIVED NEW COORDINATOR = %d\n",
                my_port, new_coordinator_port);
        
        } else if (msg_type == MSG_HEARTBEAT) {
            int ack = MSG_HEARTBEAT_ACK;
            send_all(client, (char*)&ack, sizeof(int));
        } else if (msg_type == MSG_BROADCAST_NEW_NODE) {

            char buffer[256];
            int bytes = recv_all(client, buffer, sizeof(buffer)-1);
            if (bytes <= 0) {
                closesocket(client);
                continue;
            }

            buffer[bytes] = '\0';

            char name[50], ip[50];
            int port;

            sscanf(buffer, "%s %s %d", name, ip, &port);

            printf("Received new node: %s %s %d\n", name, ip, port);

            merge_node(name, ip, port);
        } else if (msg_type == MSG_REQUEST_NODE_LIST) {
            send_node_list(client);
        } else if (msg_type == MSG_UPDATE_NODE_LIST) {

            char buffer[2048];
            int bytes = recv_all(client, buffer, sizeof(buffer)-1);

            if (bytes > 0) {
                buffer[bytes] = '\0';
                update_node_list(buffer);
            }
        }

        closesocket(client);
    }
}




int main() {

    char self_name[50];
    char self_ip[32] = "127.0.0.1";

    int my_port;
    char port_input[10];

    // Get hostname and port
    get_string("Enter hostname (must be unique):", self_name, 50);
    my_port = get_int("Enter port (must be unique):");

    if (strlen(self_name) == 0) {
        printf("Name cannot be empty\n");
        return 1;
    }

    // validation
    if (my_port <= 0 || my_port > 65535) {
        printf("Invalid port number\n");
        return 1;
    }

    

    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);

    server_socket = socket(AF_INET, SOCK_STREAM, 0);

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(my_port);

    bind(server_socket, (struct sockaddr*)&address, sizeof(address));
    listen(server_socket, 5);

    printf("\nNode started:\n");
    printf("Name: %s\n", self_name);
    printf("IP: %s\n", self_ip);
    printf("Port: %d\n", my_port);

    Node new_node;
    strcpy(new_node.name, self_name);
    strcpy(new_node.ip, self_ip);
    new_node.port = my_port;


    load_nodes_from_file();
    node_self_register(self_name, self_ip, my_port);
    append_node_to_file(new_node, self_name);

    // wait before election
    Sleep(1000);

    if (discover_coordinator(self_ip, my_port)) {
        printf("Coordinator found at port %d\n", coordinator_port);
    } else {
        printf("No coordinator found -> starting election\n");
        start_election(my_port);
    }

    while (1) {

        if (is_coordinator) {
            run_as_coordinator(my_port);
        } else {
            run_as_worker(self_ip, my_port);
        }

    }

    closesocket(server_socket);
    WSACleanup();

}
