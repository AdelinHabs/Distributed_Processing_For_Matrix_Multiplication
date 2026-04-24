#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <winsock2.h>

#pragma comment(lib, "ws2_32.lib")

#define COORDINATOR_IP "127.0.0.1"
#define COORDINATOR_PORT 6000

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

// Message header
typedef struct {
    int rowsA;
    int colsA;
    int rowsB;
    int colsB;
} MessageHeader;

int matrix_A_rows_count, matrix_A_columns_count, matrix_B_rows_count, matrix_B_columns_count;


int send_all(SOCKET sock, char *buffer, int length) {
    int total = 0;
    int bytes;

    while (total < length) {
        bytes = send(sock, buffer + total, length - total, 0);

        if (bytes <= 0) {
            return -1;
        }

        total += bytes;
    }

    return total;
}

int recv_all(SOCKET sock, char *buffer, int length) {
    int total = 0;
    int bytes;

    while (total < length) {
        bytes = recv(sock, buffer + total, length - total, 0);

        if (bytes <= 0) {
            return -1;
        }

        total += bytes;
    }

    return total;
}


int main() {

    // -------------------- Initializing Networking ---------------------
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);

    // ------------------Getting the Matrix sizes from the user--------------------------
    while (1) {

        printf("\n\nWe shall multiply A x B\nEnsure the columns of A are equal to the rows of B");
        printf("\nEnter the number of the rows of the matrix A: ");
        scanf("%d", &matrix_A_rows_count);

        printf("\nEnter the number of the columns of the matrix A: ");
        scanf("%d", &matrix_A_columns_count);

        printf("\nEnter the number of the rows of the matrix B: ");
        scanf("%d", &matrix_B_rows_count);

        printf("\nEnter the number of the columns of the matrix B: ");
        scanf("%d", &matrix_B_columns_count);

        if (matrix_A_columns_count == matrix_B_rows_count){
            break;
        } else {
            printf("\n\n!!!ERROR!!!\nThe columns of A should be equal to the rows of B");
        }
    }


    // ------------Setting sizes of the matrices using the values from the user--------------------
    int A[matrix_A_rows_count][matrix_A_columns_count];
    int B[matrix_B_rows_count][matrix_B_columns_count];
    int result[matrix_A_rows_count][matrix_B_columns_count];



    // -------------------Matrix A-------------------------
    printf("\n\nEnter Matrix A:\n");
    for (int i = 0; i < matrix_A_rows_count; i++){
        for (int j = 0; j < matrix_A_columns_count; j++){
            scanf("%d", &A[i][j]);
        }
    }
    
    // -------------------Matrix B-------------------------

    printf("\n\nEnter Matrix B:\n");
    for (int i = 0; i < matrix_B_rows_count; i++){
        for (int j = 0; j < matrix_B_columns_count; j++){
            scanf("%d", &B[i][j]);
        }
    }

    
    // ------------------- Connect to Coordinator ------------------
    SOCKET coordinator_socket = socket(AF_INET, SOCK_STREAM, 0);
    

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(COORDINATOR_PORT);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    printf("\nConnecting to Coordinator...\n");

    if (connect(coordinator_socket, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        printf("Failed to connect to coordinator\n");
        return 1;
    }

    printf("Connected to Coordinator.\n");


    MessageHeader header;
    header.rowsA = matrix_A_rows_count;
    header.colsA = matrix_A_columns_count;
    header.rowsB = matrix_B_rows_count;
    header.colsB = matrix_B_columns_count;

    int msg = MSG_TASK;
    printf("\nSending msg\n");
    if (send_all(coordinator_socket, (char*)&msg, sizeof(int)) <= 0) {
        printf("Failed to send type\n");
        return 1;
    }

    // printf("\nSending A rows\n");
    // if (send_all(coordinator_socket, (char*)&matrix_A_rows_count, sizeof(int)) <= 0) {
    //     printf("Failed to send rows of A\n");
    //     return 1;
    // }
    // printf("\nSending A columns\n");
    // if (send_all(coordinator_socket, (char*)&matrix_A_columns_count, sizeof(int)) <= 0) {
    //     printf("Failed to send columns of A\n");
    //     return 1;
    // }
    // printf("\nSending B rows\n");
    // if (send_all(coordinator_socket, (char*)&matrix_B_rows_count, sizeof(int)) <= 0) {
    //     printf("Failed to send rows of B\n");
    //     return 1;
    // }
    // printf("\nSending B columns\n");
    // if (send_all(coordinator_socket, (char*)&matrix_B_columns_count, sizeof(int)) <= 0) {
    //     printf("Failed to send columns of B\n");
    //     return 1;
    // }






    printf("\nSending Header\n");
    if (send_all(coordinator_socket, (char*)&header, sizeof(header)) <= 0) {
        printf("Failed to send header\n");
        return 1;
    }

    // Send Matrix A
    printf("\nSending Matrix A\n");
    if (send_all(coordinator_socket, (char*)A, sizeof(int) * matrix_A_rows_count * matrix_A_columns_count) <= 0) {
        printf("Failed to send matrix A\n");
        return 1;
    }

    // Send Matrix B
    printf("\nSending Matrix B\n");
    if (send_all(coordinator_socket, (char*)B, sizeof(int) * matrix_B_rows_count * matrix_B_columns_count) <= 0) {
        printf("Failed to send matrix B\n");
        return 1;
    }

    // Receive message type and Result
    int msg_type;

    if (recv_all(coordinator_socket, (char*)&msg_type, sizeof(int)) <= 0) {
        printf("Failed to receive response\n");
        return 1;
    }

    if (msg_type == MSG_RESULT) {
        if (recv_all(coordinator_socket, (char*)result, sizeof(int) * matrix_A_rows_count * matrix_B_columns_count) <= 0) {
            printf("Failed to receive result matrix\n");
            return 1;
        }
    }

    // ---------------------FINAL MATRIX-------------------------
    printf("\nFinal Result Matrix:\n");

    for (int i = 0; i < matrix_A_rows_count; i++) {
        for (int j = 0; j < matrix_B_columns_count; j++) {
            printf("%d ", result[i][j]);
        }
        printf("\n");
    }
    
    closesocket(coordinator_socket);

    WSACleanup();
    return 0;
}