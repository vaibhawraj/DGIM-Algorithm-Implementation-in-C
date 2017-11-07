/*
	Filename : client.c
	Description : Main program to receive streaming data from server and answer query on separate thread

*/

//Header File
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <termios.h>

#define R 3

//Structures
typedef struct {
	int windowSize;
	int N;					//Size of Total Bits
	int recentTimestamp;
	char host_details[56];
	char* host;
	char* port;
	void* bucketTable;
	void* window;
}input_t;

typedef struct list_elem_s{
		void *data;
		struct list_elem_s *next;
		struct list_elem_s *prev;	
}list_elem_t;

typedef struct {
	list_elem_t *first;
	list_elem_t *last;
	int size;
}list_t;

typedef struct {
	char data;
	int timestamp;
}window_t;

typedef struct {
	int size;	//size of 2
	int recentTimestamp;
	int lastTimestamp;
}bucket_t;

//Global Variable
pthread_mutex_t lock;
struct termios old,new;
int isPrinting = 0;

//Function Defination
void *client_receiver_thread(void *arg);
void *query_thread(void *arg);

int connectToServer(char*, int);

char* readQuery();
int parseQuery(char *str);
int indexOf(char *string, char s);
char* substr(char *string, int s, int d);
void toUpperCase(char *str);

void updateWindow(list_t *window, char ch, int timestamp, int max_window_size);
void updateBucketTable(list_t *bucketTable, int timestamp);
void computeCountUsingWindow(list_t *window,int bits);
void computeCountUsingBucket(list_t *bucketTable, int timestamp, int bits);

void set_terminal_mode();
void reset_terminal_mode();
int kbhit();

//Main Program
int main(int argc, char **argv) {
	pthread_t reciever_thread_ID, query_thread_ID ;
	void *exitstatus;
	input_t input;
	list_t bucketTable, window;

	//Initialize Window & Bucket
	window.size = 0;
    window.first = NULL;
    window.last = NULL;

    bucketTable.size = 0;
    bucketTable.first = NULL;
    bucketTable.last = NULL;

	input.windowSize = 0;
	input.window = &window;
	input.bucketTable = &bucketTable;

	//Read Window Size & Host details
	scanf("%d", &input.windowSize);
	scanf("%s", input.host_details);

	//Create Thread & Mutex
	pthread_mutex_init(&lock, NULL);
	pthread_create(&reciever_thread_ID, NULL, client_receiver_thread, &input);
	pthread_create(&query_thread_ID, NULL, query_thread, &input);

	//Wait for threads to end
	pthread_join(reciever_thread_ID, &exitstatus);
	pthread_join(query_thread_ID, &exitstatus);
	
	return 0;
}

//Thread Function to receive data from the server
void *client_receiver_thread(void *arg) {
	int conn,timestamp;
	input_t *input = ((input_t *) arg);
	list_t *window = (list_t *) input->window;
	list_t *bucketTable = (list_t *) input->bucketTable;

    //Temporary Variables
    list_elem_t *ptr;
    char ch;
    int i;

    //Parse Host & Port from input parameter
    if(indexOf(input->host_details,':')>0) {
    	input->host = substr(input->host_details,0,indexOf(input->host_details,':'));
    	input->port = substr(input->host_details,indexOf(input->host_details,':')+1,strlen(input->host_details));
    } else {
    	fprintf(stderr, "Improper format for host:port\n");
    	exit(0);
    }

    //Connect to server
    conn = connectToServer(input->host,atoi(input->port));

	//Initialize Window & DGIM Bucket Table
    if(input->windowSize <= 0) {
    	fprintf(stderr, "Window Size cannot be negative\n");
    	exit(0);
    }

    timestamp = 0;
    input->N = timestamp;
    input->recentTimestamp = timestamp;

	//Start reading data from network stream
	while(read(conn,&ch,1) > 0) {
		if(ch!='\n') {
			pthread_mutex_lock(&lock);

			isPrinting = 1;
			printf("%c",ch);
			fflush(stdout);
			
			if(ch=='1' || ch=='0') {
				//Update Window
				updateWindow(window,ch,timestamp,input->windowSize);
				if(ch=='1') {
					//Update DGIM Bucket Table
					updateBucketTable(bucketTable, timestamp);
				}
				timestamp++;
				input->N = timestamp;
				input->recentTimestamp = timestamp;
			}
			pthread_mutex_unlock(&lock);
		}
	}
	isPrinting = 0;
	printf("\n");
	//reset_terminal_mode();
	fflush(stdout);

	//Close,Clean and return
	close(conn);
	return NULL;
}

//Thread Function to receive query from the server
void *query_thread(void *arg) {
	char* query;
	int k;
	input_t *input = ((input_t *) arg);
	
	set_terminal_mode();	//Disable echo on terminal
	
	while(1) {
		if(kbhit()) {
			pthread_mutex_lock(&lock);
				reset_terminal_mode();	//After grabbing lock, it will enable echo on terminal
				
				query = readQuery();
				if(strlen(query)!= 0) {
					k = parseQuery(query);
			
					if(k==-1) {
						printf("Unable to parse query statement\n");
					} else if(k<=input->windowSize) {
						//Use Window for computing one
						computeCountUsingWindow(input->window,k);
					} else {
						//Use DGIM Bucket Table for computing one
						computeCountUsingBucket(input->bucketTable,input->recentTimestamp, k);
					}
				}
				free(query);
				set_terminal_mode();  //Disable echo on terminal again
			pthread_mutex_unlock(&lock);
		}
	}
	return NULL;
}

void updateWindow(list_t *window, char ch, int timestamp, int max_window_size) {
	window_t *windowElem = (window_t *) malloc(sizeof(window_t));
	list_elem_t *list_elem = (list_elem_t *) malloc(sizeof(list_elem_t)),*temp;

	//Create WindowElem
	windowElem->data = ch;
	windowElem->timestamp = timestamp;

	//Create List Elem
	list_elem->data = windowElem;
	list_elem->next = NULL;
	list_elem->prev = NULL;

	//Update Window List
	if(window->first != NULL)
		window->first->prev = list_elem;
	list_elem->next = window->first;
	window->first = list_elem;
	if(window->last == NULL)
		window->last = list_elem;
	window->size++;

	//Validate window length
	if(window->size > max_window_size) {
		if(window->last!=NULL) {
			temp = window->last;
			window->last = temp->prev;
			if(temp->prev != NULL)
				temp->prev->next = NULL;

			free(temp->data);
			free(temp);

			window->size--;
			if(window->size == 0) {
				window->first = NULL;
				window->last = NULL;
			}
		}
	}
}

void updateBucketTable(list_t *bucketTable, int timestamp) {
	//Bucket will be updated whenever a new 1 comes
	bucket_t *bucket,*b1,*b2;
	list_elem_t *list_elem,*ptr, *next_ptr;
	int count;

	//Create New Bucket
	bucket = (bucket_t *) malloc(sizeof(bucket_t));
	bucket->size = 1;
	bucket->recentTimestamp = timestamp;
	bucket->lastTimestamp = timestamp;

	//Create list item
	list_elem = (list_elem_t *) malloc(sizeof(list_elem_t));
	list_elem->data = bucket;
	list_elem->next = NULL;
	list_elem->prev = NULL;

	//Add it to BucketTable
	if(bucketTable->size == 0) {
		bucketTable->first = list_elem;
		bucketTable->last = list_elem;
		bucketTable->size++;
	} else {
		list_elem->next = bucketTable->first;
		bucketTable->first->prev = list_elem;
		bucketTable->first = list_elem;
		bucketTable->size++;
	}

	//Maintain DGIM condition
	if(bucketTable->first != NULL) {
		ptr = bucketTable->first;
		count = 0;
		while(ptr!=NULL && ptr->next!=NULL) {
			next_ptr = ptr->next;
			b1 = (bucket_t *) ptr->data;
			b2 = (bucket_t *) next_ptr->data;
			
			if(b1->size == b2->size) {
				count++;
			} else {
				count = 0;
			}
			if(count == R) {
				//Merge
				b1->size = b1->size + b1->size;
				b1->lastTimestamp = b2->lastTimestamp;

				//Remove B2 bucket
				ptr->next = next_ptr->next;
				if(next_ptr->next != NULL) {
					next_ptr->next->prev = ptr;
				} else {
					bucketTable->last = ptr;
				}

				count = 1;

				free(b2);
				free(next_ptr);

			}
			ptr = ptr->next;
		}
	}
}

void computeCountUsingWindow(list_t *window,int bits) {
	list_elem_t *ptr;
	window_t *w;
	int count = 0,k=0;
	ptr = window->first;
	while(ptr!=NULL && k < bits) {
		w = (window_t *)ptr->data;
		if(w->data == '1') count++;
		k++;
		ptr = ptr->next;
	}
	printf("The number of ones of last %d data is exact %d\n",bits,count);
	//printf("%d\n",count);
}

void computeCountUsingBucket(list_t *bucketTable, int timestamp, int bits) {
	list_elem_t *ptr;
	bucket_t *b;
	int count = 0,isExact = 1;
	int limitTimestamp = timestamp - bits;
	//printf("Debug[Bucket] t=%d t'=%d bits=%d\n",timestamp,limitTimestamp,bits);
	ptr = bucketTable->first;
	while(ptr!=NULL) {
		b = ptr->data;
		//printf("Bucket %d[%d %d]\t",b->size,b->recentTimestamp,b->lastTimestamp);
		if(limitTimestamp < b->lastTimestamp) {
			count = count + b->size;
			//printf("Less than old timestamp %d\n",count);
		} else if(limitTimestamp == b->lastTimestamp) {
			count = count + b->size;
			//printf("All Boundary condition %d\n",count);
			isExact = 1;
			break;
		} else if(limitTimestamp < b->recentTimestamp) {
			if(b->size == 1) {
				count = count + b->size;
				isExact = 1;
			} else {
				count = count + (b->size/2);
				isExact = 0;
			}
			//printf("Inbetween condition %d\n",count);
			break;
		} else if(limitTimestamp == b->recentTimestamp) {
			count = count + 1;
			//printf("One Boundary condition %d\n",count);
			isExact = 1;
			break;
		} else {
			break;
		}
		ptr = ptr->next;
	}
	if(isExact) {
		printf("The number of ones of last %d data is exact %d\n",bits,count);
	} else {
		printf("The number of ones of last %d data is estimated %d\n",bits,count);
	}
}


//Helper Methods
char* readQuery() {
	int i = 0, k,n;
	char *str,ch;
	char buff[255];
	str = (char *) malloc(sizeof(char) * 255);
	i=0;
	str[i] = 0;
	int firstTime = 1;
	for(k=0;k<255;k++) {
		buff[k] = 0;
	}
	do{
		if((n=read(0,buff,sizeof(buff))) > 0) {
			//printf("[%d]",isPrinting);
			if(isPrinting) {
				if(buff[0]!='\n')
					printf("\n");
				isPrinting = 0;
			}
			if(firstTime) {
				printf("%s",buff);
				firstTime = 0;
				fflush(stdout);
			}
			//
			for(k=0;k<n;k++) {
				ch = buff[k];
				if(ch=='\n')
					str[i]=0;
				else
					str[i]=ch;
				i++;
			}
		}
	}while(ch!='\n');
	str[i] = 0;
	return str;
}

int parseQuery(char *str) {
	int k = -1,i;
	char queryFormat[] = "WHAT IS THE NUMBER OF ONES FOR LAST ";
	int queryLength = strlen(queryFormat);
	char *temp,*temp2;
	if(str == NULL) return -1;
	if(strlen(str) > queryLength) {
		temp = substr(str,0,queryLength);
		toUpperCase(temp);
		//printf("%s",str);
		if(strcmp(temp,queryFormat) == 0) {
			free(temp);
			temp = substr(str,queryLength,strlen(str));
			if(strlen(temp)!=0) {
				i = indexOf(temp,' ');
				if(i == -1) {
					free(temp);
					return -1; //Invalid Query
				} else {
					temp2 = substr(temp,i+1, strlen(temp));
					toUpperCase(temp2);
					if(strcmp(temp2,"DATA?") != 0) {
						free(temp2);
						return -1;
					}
					free(temp2);
					temp2 = substr(temp,0,i);
				}
				free(temp);
				//See if temp2 is number
				for(i=0;i<strlen(temp2);i++) {
					if(!(temp2[i] >= '0' && temp2[i] <= '9')){
						free(temp2);
						return -1;
					}
				}
				k = atoi(temp2);
				free(temp2);
			}
		}
	}
	return k;
}

void toUpperCase(char *str) {
	int i=0;
	for(i=0;i<strlen(str);i++) {
		if(str[i] >= 'a' && str[i] <= 'z') {
			str[i] = str[i] - ('a' - 'A');
		}
	}
}

int connectToServer(char *host, int portno) {
	int sockfd;
	struct sockaddr_in serv_addr;
    struct hostent *server;

	//Create Socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
    	fprintf(stderr, "ERROR opening socket");
    	exit(0);
    }

    server = gethostbyname(host);
    if (server == NULL) {
        fprintf(stderr,"ERROR, no such host\n");
        exit(0);
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, 
         (char *)&serv_addr.sin_addr.s_addr,
         server->h_length);
    serv_addr.sin_port = htons(portno);

    /////Connect with the server
    if (connect(sockfd,(struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0) {
        fprintf(stderr,"ERROR connecting\n");
        exit(0);
    }

    return sockfd;
}

//String Helper Methods
int indexOf(char *string, char s) {
	int index = -1,i;
	for(i=0;i<strlen(string);i++) {
		if(string[i]==s) {
			index = i;
			break;
		}
	}
	return index;
}

char* substr(char *string, int s, int d) {
	char *str;
	int len=0,i;
	if(d < s) {
		len = 0;
	} else
		len = d-s+1;
	str = (char *) malloc(sizeof(char)*(len));	//Extra for null character
	for(i=s;i<d;i++) {
		str[i-s] = string[i]; 
	}
	str[i-s] = 0;
	return str;
}

/* Input methods */

void set_terminal_mode() {
	/* Retrieve current terminal settings, turn echoing off & icanon */
    if (tcgetattr(STDIN_FILENO, &new) == -1) {
    	printf("tcgetattr error\n");
        exit(0);
    }
    old = new;                          /* So we can restore settings later */
	new.c_lflag &= ~(ICANON | ECHO);
	atexit(reset_terminal_mode);

	if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &new) == -1) {
    	printf("tcsetattr error\n");
        exit(0);
    }
}

int kbhit() {
	int retval;
	fd_set rfds;
    struct timeval tv;

	/* Watch stdin (fd 0) to see when it has input. */
	FD_ZERO(&rfds);
    FD_SET(0, &rfds);
    /* Wait up to five seconds. */
    tv.tv_sec = 0;
    tv.tv_usec = 0;
	retval = select(1, &rfds, NULL, NULL, &tv);
	return retval;
}

void reset_terminal_mode() {
	tcsetattr(0, TCSANOW, &old);
}
