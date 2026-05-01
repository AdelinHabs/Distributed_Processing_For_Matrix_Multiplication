# Distributed Matrix Multiplication System

## 1. Overview
This system implements a distributed computing model for matrix multiplication (C = A × B). It consists of a client, multiple nodes, and a dynamically elected coordinator. Tasks are distributed across worker nodes to improve performance and fault tolerance.  
This system has capabilty to run on Windows, Linux and Android.
##### Note:  
- For Windows, use the files in the windows folder.
- For Linux, use the files in the linux folder.
- For Android, use the files in the linux folder.

---

## 2. System Architecture
The system follows a peer-based distributed architecture where each node can act as either a worker or coordinator.

- **Coordinator**: Distributes tasks, collects results, and manages workers.
- **Worker Node**: Performs assigned matrix multiplication tasks.
- **Client**: Submits matrix multiplication requests.

---

## 3. Key Components

- **Client**
  - Sends matrix multiplication tasks to the coordinator.

- **Node**
  - Can act as a worker or coordinator depending on election outcome.

- **Coordinator**
  - Distributes computation tasks
  - Collects results
  - Handles node discovery and coordination

- **Worker**
  - Computes assigned rows of matrix multiplication

---

## 4. Communication Protocol
The system uses:

- **TCP sockets** → Reliable communication between nodes
- **UDP broadcast** → Coordinator discovery

### Message Types (Integer-Based Protocol)
- `MSG_TASK (1)` → Assign computation task  
- `MSG_RESULT (2)` → Return computed result  
- `MSG_ELECTION (3)` → Election request  
- `MSG_COORDINATOR (4)` → Coordinator announcement  
- `MSG_HEARTBEAT (5)` → Health check  
- `MSG_HEARTBEAT_ACK (6)` → Heartbeat response  
- `MSG_ELECTION_OK (7)` → Higher priority node exists  

---

## 5. Coordinator Discovery Mechanisms
The client locates the coordinator using:

- UDP Broadcast (`WHO_IS_COORDINATOR`)
- Scanning `nodes.txt` with heartbeat checks

This ensures reliability even when some nodes fail.

---

## 6. Heartbeat and Failure Detection
- Nodes periodically send heartbeat messages.
- If the coordinator fails to respond:
  - Election is triggered automatically
  - A new coordinator is selected

---

## 7. Election Algorithm
The system uses a **Bully Algorithm**:

- Higher port number = higher priority
- If coordinator fails:
  - Nodes initiate election
  - Highest alive node becomes coordinator

---

## 8. Task Distribution Strategy
- Matrix multiplication is split by **rows of Matrix A**
- Each worker computes one or more rows
- Results are sent back to the coordinator
- If no workers are available, coordinator computes locally

---

## 9. Setup Environment

### 9.1 Windows Setup  
#### Requirements
- GCC (MinGW or MSYS2)
- `ws2_32` library

#### Compilation
```bash
gcc node.c -o node.exe -lws2_32
gcc client.c -o client.exe -lws2_32
```

#### Running:
For Client,
```bash
./client
```
Then follow on-screen instructions.

For Node,
```bash
./node
```
You will be prompted to enter:  
-Unique node name  
-Unique Port number


---

### 9.2 Linux Setup

#### Compilation
```bash
gcc node.c -o node -lpthread
gcc client.c -o client -lpthread
```

#### Running
For Client,
```bash
./client
```
Then follow on-screen instructions.

For Node,
```bash
./node
```
You will be prompted to enter:  
-Unique node name  
-Unique Port number


---

### 9.3 Android Setup (Termux)

-	Install the app Termux from Google Play Store.  
-	Proceed to ensure that the files are in a folder on the phone and that the folder are in the $HOME/ folder.  
-	If the folder having the files isn’t in the $HOME/, move to the folder that contains the folder with the files and run the following commands.  

mv folder_with_files $HOME/
cd $HOME/folder_with_files
pkg install clang
clang node_linux.c -o node
clang client_linux.c -o client

#### Running:
./node
./client

---

## 10. Running the System

#### Note:  
Ensure that the every node is aware of the other by making sure the nodes.txt on evry node is populated with the details of the available nodes before running.  

Steps to run:  

1. Start multiple nodes
2. Ensure nodes register in nodes.txt
3. Allow coordinator election
4. Run client
5. Enter matrix values
6. System distributes computation

---

## 11. nodes.txt Format

node_name IP_address port

Example:
```bash
node1 192.168.1.10 5000
```

#### Note:  
Ensure that the every node is aware of the other by making sure the nodes.txt on evry node is populated with the details of the available nodes before running.  

---

## 12. Fault Tolerance

- Heartbeat failure triggers election
- New coordinator selected automatically
- Client reconnects dynamically

---

## 13. Assumptions

- Unique IP/port per node
- Reliable TCP communication
- Integer matrices only
- Accessible nodes.txt

---

## 14. Conclusion

Demonstrates distributed computing concepts:
- Parallel processing
- Leader(Coordinator) election
- Fault tolerance
- Task distribution
