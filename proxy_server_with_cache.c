#include "proxy_parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/wait.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

#define MAX_BYTES 4096    //max allowed size of request/response
#define MAX_CLIENTS 400     //max number of client requests served at a time

//why this much size--
//SOLUTION--
// Memory Safety:

// In low-level C programs, especially when doing things like caching in proxies or servers, you need to predefine limits so the cache doesn’t consume infinite memory and crash the server.

// Realistic Cache Size:

// 200 MB is a reasonable size to hold several web objects (HTML pages, CSS files, etc.).

// It balances performance (larger cache = more hits) and resource usage (RAM consumed).

// Manual Control:

// In educational or simple implementations, you manually fix cache size for simplicity.

// In production systems, cache size may be configurable via a .conf file or dynamically adjusted based on system memory.
#define MAX_SIZE 200*(1<<20)     //size of the cache 200*2^20
#define MAX_ELEMENT_SIZE 10*(1<<20)     //max size of an element in cache  10* 2^10----------1gb k ass pas

typedef struct cache_element cache_element;

//jo cache bna rha hun usme element store krne hai
//jo store krna hai vo struct me define krnge
//jo v isme element store krnge iska mtlb ki vo phle kbhi na kbhi aya tha for sure tbhi na cache me hai vo
//cache ka size v limited rkhenge hamlog
//unlimited store nhi kr skte hmlog usme..
struct cache_element{
    char* data;         //jo v data ay atha response me usko store krnge ham
    int len;          //length of data i.e.. sizeof(data).....kitne length ka data hai
    char* url;        //url stores the request...kon se url pe request gayi thi...suppose ki hamri url get request thi and url was http//google.com to usko v yha tore kr lunga
	//taki jab hamra request dubara google.com se aygi to easy hga mere liye us element ko find krna us url se...

	//ham time based lru cache use kr rhe hnge
	//iske liye we have included time.h in header file
	//time based mtlb...ki agr let cache ka size==3 hai and let 4th element ka request ata hai...www.fb.com
	//to aap jab server se uska response lekr aoge to usko return krne se phle cache me store krna chahoge
	//par cache to filled hai already
	//to mai LRU me dekhunga ki sbse purana element kon sa hai..least recently used...recent time me sbse kam bar use hona is least recently used cache

	//sbse last me jiska time hai..use hua tha pr time bhot jada ho gya usi ko remove krnge ham

	time_t lru_time_track    //lru_time_track stores the latest time the element is  accesed
;
	//and best data structure to store or perform caching is linked list
	//to in c langauge ham linked list bna rhe hai...like in c++ linked list me hr element ka next pointer hota tha
	//waise hi yha pe chuki struct type ka hai to struct type ka next pointer
	// struct cache_element *next;
	//maine  struct cache_element isko upr define kr diya hai header me ----line 24(typedef struct cache_element cache_element;)

    cache_element* next;    //pointer to next element
};


//ham kuch function bnaynge jo is linked list jo cache bna rhe hai ham linked list type ka ...to obvious koi v request ayega to phle cache me find krna to pdega na
//isliye phle ham na cache find krnge original server pe jane se phle
//islye we are making this function...we will delare it further
cache_element* find(char* url);

//again wahi linked list hai...koi v naya aya element to usko add to krna pdega a cache me yar....and jaise bola tumko upr me struct me dekh
//hr node will contain-- data--size of data and url of it...
//function declare krna rhe hain
int add_cache_element(char* data,int size,char* url);

//ovbious hai jab lru cache ka size full ho jiga to we need to remove some data from it na
//to uske liye ye function bnainge hamlog
//to obvious ye koi argument nhi lega...kyuki simply delete kr na hai ye mujhe
void remove_cache_element();


//jo hmara khud ka proxy server jo chalega vo ham na 8080 port pe chalinge
int port_number = 8080;				// Default Port

//to har bar jab proxy se connect krna hoga to use socket open krna padega...socket communication to krni pdegi 
//iske liye ham na ek socketid bna lenge global level pe apne proxy ke liye bana denge
int proxy_socketId;					// socket descriptor of proxy server

//chuki ye multithreaded hai.......to jab GOOGLE.COM aya--- to ek socket open krne wala hun(socket programng me smjh me ayega CN ka copy me hai)
//too lettt-- C1 client hai usne google.com request kiya
//and same time pe C2 client request krta hai facebook

//to ye dono request proxy server pe jaigi...kyuki mera multithreaded proxy web server hai to obvious multiple request accept krni pdegi na
//to jab dono sath me jainge bhai server pe...to dono k liye alg socket kholna hoga na ji...
//mtlb ki google k liye ek alg socket and facebook k liye alg socket kholni pdegi
//to mai let google.com ka request accept krunga  and iske liye ek naya socket bna dunga--to jo v response ayega cache me store hone k bad isi socket se return kr dunga mai
//and ye sare socket mere proxy server se connected honge

//isi ke liye we will define thread here
//jinte mere client yha connect honge utne mai thread bnaunga
//and har ek thread me 1 socket bana hua hga
//to ham ye define kr dnge manually ki ham kitne client ko 1 sath lena chahte hai
//to ham upr MAX_CLIENT define kr rkhe hai ki kitne client 1 bar me connect ho xkte hai
//chulki we are using pthred isliye wee need to include the library of it #include<pthread.h>
//array isliye bnaya thread ka ki jitne client hai utne thread taki 1 bar me sab request behj sake ans har ek me thread id store hogi array me



pthread_t tid[MAX_CLIENTS];         //array to store the thread ids of clients


//lets see ki inka kya hai role--
//lets say ki chuki hamara LRU cache hai na
//let C1 client google.com request krta hai and c2 client fb.com request krta hai
//jab google.com request krta hai to server se response lakr cache me dal rha hunga
//also chuki thread hai na to parallely fb.com v usi cache me dal rha hga
//to overall cache becomes our shared memory right?
//to is situation me aa skta hai race condition
//and jab v race condition ati hai to ham usko handle krte hai lock se
//let t1 thread google k liye hai
//let t2 thread fb k liye hai
//  to T1 dekhega cache pe ki kya kisi ne phle lock kr rkha hai kya..agr lock acquired hai to wait krega until dusra thread relese na kar de wrna le lega
//same T2 thread v akr yhi dekhega...agr lock hoga to rukega wrna le lega
//to ye sb k liye hame mutex lock ye sab ka jruri pdta hai


//WHY SEMAPHORE???--
//semaphore v lock ka type hi hai but ye ek variable hota hai 
// semaphore wait() and semaphore signal() do trh ke hote hai 
//like ki semaphore--MAX_CLIENT k itna kr dunga ise----(for my reference ek bar semaphore padh lena before interview from gate smasher so that clearity ache se ho jaye is chij ki)
//thread id ka array v max_client ka itna hi hai
//sem_wait()--- let 1 client ki request aygi to mai thread array me dekhunga ki semaphore ki kitni value hai...agr >0 hai to wait() me chla jaunga and semaphore ka vlaue-- kr dunga....
//ye tab tak krega until semaphore >0 
//and tb tk ye andr wait() krne k bad jo v krna tha vo krega andr jo v krna tha
//and agr koi exit ho jata hai mtlb puri trh se run kr gya agr to sem_signal() se exit krega and semaphore ka value++ krega

//PLZ I WILL SUGGEST KI SEMAPHORE KI EK BAR CONCEPT DEKH LENA KI KYA KRTA HAI YE AKHIR ME

//lets say semaphore ki value==0 ho gyi...and then ek client ata hai---to sem_wait() call krunga...pr sem_wait() me to dekhega ki <=0 hai ye to and tab tak wait krte rhega jab tk ise koi positive na kr de....by sem_signal() koi exit krke


//semaphore is just like lock only-- but only the diference is mutex lock has only 2 value in it--0 or 1
//but semaphore has multiple value for locks

//WHY SEMAPHORE BUT NOT MUTEX IF BOTH ARE SERVED AS LOCK ONLY THEN---

//solution-----
// Multiple Resources (Counting Semaphore):
// A semaphore can be initialized to N, allowing up to N threads to access a resource concurrently. A mutex only allows one.

// 👉 Example: You have 5 printers. You want to allow at most 5 print jobs at once. A mutex can't help here — but a counting semaphore initialized to 5 can.

//for including sempahore.h we have included its header i.e sempahore.h
sem_t seamaphore;                 //if client requests exceeds the max_clients this seamaphore puts the
                                    //waiting threads to sleep and wakes them when traffic on queue decreases
//sem_t cache_lock;			       
pthread_mutex_t lock;               //lock is used for locking the cache





//bhai linked list se implement kr rhe hai to globally uska head hona chaiye na taki point krta rhe hmesha head pe
//vo define kr diya yaha pe--
cache_element* head;                //pointer to the cache

int cache_size;             //cache_size denotes the current size of the cache

int sendErrorMessage(int socket, int status_code)
{
	char str[1024];
	char currentTime[50];
	time_t now = time(0);

	struct tm data = *gmtime(&now);
	strftime(currentTime,sizeof(currentTime),"%a, %d %b %Y %H:%M:%S %Z", &data);

	switch(status_code)
	{
		case 400: snprintf(str, sizeof(str), "HTTP/1.1 400 Bad Request\r\nContent-Length: 95\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>400 Bad Request</TITLE></HEAD>\n<BODY><H1>400 Bad Rqeuest</H1>\n</BODY></HTML>", currentTime);
				  printf("400 Bad Request\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 403: snprintf(str, sizeof(str), "HTTP/1.1 403 Forbidden\r\nContent-Length: 112\r\nContent-Type: text/html\r\nConnection: keep-alive\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>403 Forbidden</TITLE></HEAD>\n<BODY><H1>403 Forbidden</H1><br>Permission Denied\n</BODY></HTML>", currentTime);
				  printf("403 Forbidden\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 404: snprintf(str, sizeof(str), "HTTP/1.1 404 Not Found\r\nContent-Length: 91\r\nContent-Type: text/html\r\nConnection: keep-alive\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>404 Not Found</TITLE></HEAD>\n<BODY><H1>404 Not Found</H1>\n</BODY></HTML>", currentTime);
				  printf("404 Not Found\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 500: snprintf(str, sizeof(str), "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 115\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>500 Internal Server Error</TITLE></HEAD>\n<BODY><H1>500 Internal Server Error</H1>\n</BODY></HTML>", currentTime);
				  //printf("500 Internal Server Error\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 501: snprintf(str, sizeof(str), "HTTP/1.1 501 Not Implemented\r\nContent-Length: 103\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>404 Not Implemented</TITLE></HEAD>\n<BODY><H1>501 Not Implemented</H1>\n</BODY></HTML>", currentTime);
				  printf("501 Not Implemented\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 505: snprintf(str, sizeof(str), "HTTP/1.1 505 HTTP Version Not Supported\r\nContent-Length: 125\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>505 HTTP Version Not Supported</TITLE></HEAD>\n<BODY><H1>505 HTTP Version Not Supported</H1>\n</BODY></HTML>", currentTime);
				  printf("505 HTTP Version Not Supported\n");
				  send(socket, str, strlen(str), 0);
				  break;

		default:  return -1;

	}

	return 1;
}





//----------------------remote socket jo main server hoga actually me-------------------------
int connectRemoteServer(char* host_addr, int port_num)
{
	// Creating Socket for remote server ---------------------------

	int remoteSocket = socket(AF_INET, SOCK_STREAM, 0);

	if( remoteSocket < 0)
	{
		printf("Error in Creating Socket.\n");
		return -1;
	}
	
	// Get host by the name or ip address provided
     //gethostbyname() ye local me jiga wha host ki mapping hoti hai...jaise ap kbhi v dekho
	  //127.0.0.1 is locl host by default to uski mapping store ki hui hoti hai...ki hostname localhost jab v hoga jo uska IP hga 127.0.0.1 o ham local me search kr lete hai ki jo host ap search kr rhe ho usko ap lekr aa poage ya nhi

	struct hostent *host = gethostbyname(host_addr);	
	//agr nhi leke aa paoge to dikt nhi hai ham usi pe request marenge
	if(host == NULL)
	{
		fprintf(stderr, "No such host exists.\n");	
		return -1;
	}

	// inserts ip address and port number of host in struct `server_addr`
	struct sockaddr_in server_addr;

	bzero((char*)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port_num);

	//server k adress me jo v apka host hai..by default wahi rehega jo apne IP de rkha hai
	bcopy((char *)host->h_addr,(char *)&server_addr.sin_addr.s_addr,host->h_length);

	// Connect to Remote server ----------------------------------------------------

	if( connect(remoteSocket, (struct sockaddr*)&server_addr, (socklen_t)sizeof(server_addr)) < 0 )
	{
		fprintf(stderr, "Error in connecting !\n"); 
		return -1;
	}
	//wrna connected ho chuka hai
	// free(host_addr);
	return remoteSocket;
}





//---------------------------------handling request-----------------------------
int handle_request(int clientSocket, ParsedRequest *request, char *tempReq)
{
	char *buf = (char*)malloc(sizeof(char)*MAX_BYTES);
	strcpy(buf, "GET ");
	strcat(buf, request->path);
	strcat(buf, " ");
	strcat(buf, request->version);
	strcat(buf, "\r\n");

	size_t len = strlen(buf);

	if (ParsedHeader_set(request, "Connection", "close") < 0){
		printf("set header key not work\n");
	}

	if(ParsedHeader_get(request, "Host") == NULL)
	{
		if(ParsedHeader_set(request, "Host", request->host) < 0){
			printf("Set \"Host\" header key not working\n");
		}
	}

	if (ParsedRequest_unparse_headers(request, buf + len, (size_t)MAX_BYTES - len) < 0) {
		printf("unparse failed\n");
		//return -1;				// If this happens Still try to send request without header
	}

	int server_port = 80;				// Default Remote Server Port
	if(request->port != NULL)
		server_port = atoi(request->port);

	int remoteSocketID = connectRemoteServer(request->host, server_port);

	if(remoteSocketID < 0)
		return -1;

	int bytes_send = send(remoteSocketID, buf, strlen(buf), 0);

	bzero(buf, MAX_BYTES);

	bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);
	char *temp_buffer = (char*)malloc(sizeof(char)*MAX_BYTES); //temp buffer
	int temp_buffer_size = MAX_BYTES;
	int temp_buffer_index = 0;

	while(bytes_send > 0)
	{
		bytes_send = send(clientSocket, buf, bytes_send, 0);
		
		for(int i=0;i<bytes_send/sizeof(char);i++){
			temp_buffer[temp_buffer_index] = buf[i];
			// printf("%c",buf[i]); // Response Printing
			temp_buffer_index++;
		}
		temp_buffer_size += MAX_BYTES;
		temp_buffer=(char*)realloc(temp_buffer,temp_buffer_size);

		if(bytes_send < 0)
		{
			perror("Error in sending data to client socket.\n");
			break;
		}
		bzero(buf, MAX_BYTES);

		bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);

	} 
	temp_buffer[temp_buffer_index]='\0';
	free(buf);
	add_cache_element(temp_buffer, strlen(temp_buffer), tempReq);
	printf("Done\n");
	free(temp_buffer);
	
	
 	close(remoteSocketID);
	return 0;
}

int checkHTTPversion(char *msg)
{
	int version = -1;

	if(strncmp(msg, "HTTP/1.1", 8) == 0)
	{
		version = 1;
	}
	else if(strncmp(msg, "HTTP/1.0", 8) == 0)			
	{
		version = 1;										// Handling this similar to version 1.1
	}
	else
		version = -1;

	return version;
}




//-----------------------------------making thread_fn main() wale code me jakr dekh while loop k andr main proxy se jo new thread bna usme thread_fn call kr rhe hai----------------------------------------------------

//parameter me void mtlb ki int v pass kr skte ho...char v pass kr skte ho..anything
void* thread_fn(void* socketNew)
{
	//chuki hamko maxi_client itna hi thread chalana hai
	//wrna aise to threasd bante rhnge
	//isliye yha semaphaore ka use krnge and sem_wait() call krnge jisme semaphore jo ki max_client se intialize hai usko pass krnge
	//and wait() value ko -- krta hai until and unless >0 hga to ye chlega
	//agr -ve ho gya to wait krega wrna age badh jiga
// 	int p;
// 	pthread_t tid = pthread_self();
// // sem_getvalue(&semaphore, &p);
// // printf("Semaphore value before wait: %d\n", p);
// sem_getvalue(semaphore, &p);
// printf("Thread %lu: Semaphore value before wait: %d\n", tid, p);
// fflush(stdout);
// 	//sem_wait(&semaphore); 
// 	 // Wait on semaphore
// 	 if (sem_wait(semaphore) != 0) {
//         perror("sem_wait failed");
//         return NULL;
//     }

// 	//semaphore ki value ko print kr do for debug

// 	// sem_getvalue(&semaphore,&p);
// 	// printf("semaphore value:%d\n",p);


// // sem_getvalue(&semaphore, &p);
// // printf("Semaphore value after wait: %d\n", p);
//   // Get and print semaphore value after wait
//   sem_getvalue(semaphore, &p);
//   printf("Thread %lu: Semaphore value after wait: %d\n", tid, p);
//   fflush(stdout);


sem_wait(&seamaphore); 
int p;
sem_getvalue(&seamaphore,&p);
printf("semaphore value:%d\n",p);


    int* t= (int*)(socketNew);
	int socket=*t;           // Socket is socket descriptor of the connected Client
	//ab apne socket khol liya hai
	//to jo client http request bhejna chahta hai uska byte send krega....i.e kitne length ka byte hai vo v store kr lo na
	int bytes_send_client,len;	  // Bytes Transferred

	//ek buffer bna lo jisme request ayegi apki
	//to calloc se dynamically allocate kr rhe honge hamlog
	//and ham yha pe limited size of bytes ko bhej rhe hnge..yhi limitation hai iska
	char *buffer = (char*)calloc(MAX_BYTES,sizeof(char));	// Creating buffer of 4kb for a client
	
	//ye jo buffer bnaya usme v garbage value hoga usko v 0 set kr do..i.e clean kr do
	bzero(buffer, MAX_BYTES);								// Making buffer zero

	//ab revc() krna start krnge byte ko...to ham utna hi bytes recve kr skte hai jo upr banaya hai....MAX_BYTES
	bytes_send_client = recv(socket, buffer, MAX_BYTES, 0); // Receiving the Request of client by proxy server
	
	while(bytes_send_client > 0)
	{

		//jo maxax bytes hai utne length ka aye hi nhi..isliye length nikal liye hamlohg
		len = strlen(buffer);
        //loop until u find "\r\n\r\n" in the buffer--- as http request isi se end hoti hai...to jab tak ye nhi milta tab tak byte recv krte jaoge
		if(strstr(buffer, "\r\n\r\n") == NULL)
		{	
			//and buffer wha se start kroge na jitni length avi tak ho chuki hai recv ...and maxbytes se kitna bcha hai vo..i.e max_bytes-len
			bytes_send_client = recv(socket, buffer + len, MAX_BYTES - len, 0);
		}
		else{
			//wrna break agr end aa gya hai ya bytes_send <0 ho gya to
			break;
		}
	}




	// printf("--------------------------------------------\n");
	// printf("%s\n",buffer);
	// printf("----------------------%d----------------------\n",strlen(buffer));
	

		//aise upr me ham client se rewuest lekr ate hai ham
	//and is reqest ko temp request me bna lete hai copy kr rhe uisng malloc
	//as cache me search krna hai na hame
	char *tempReq = (char*)malloc(strlen(buffer)*sizeof(char)+1);
    //tempReq, buffer both store the http request sent by client
	//and yha pe copy kr dunga jo v buffer array me hai
	for (int i = 0; i < strlen(buffer); i++)
	{
		tempReq[i] = buffer[i];
	}
	
	//checking for the request in cache 
	//avi jo bnaya hia temp buffer...ye dekhnge ki ye hai ya ny cache me

	struct cache_element* temp = find(tempReq);
//agr ye temp LRU cache se mil gya tb to badhia hai guru
	if( temp != NULL){
        //request found in cache, so sending the response to client from proxy's cache
		int size=temp->len/sizeof(char);//ye temp array ka size nikal rhe hai hmlog
		int pos=0;

		//response v jitna bytes define kr rkhe hai wahi bhej rhe hai utne size ka
		char response[MAX_BYTES];
		while(pos<size){
			//response jo v bhej rhe ho phle khali kr lo isee
			bzero(response,MAX_BYTES);
			for(int i=0;i<MAX_BYTES;i++){
				response[i]=temp->data[pos];//to jo temp me data hga use return kr do...ye temp na struct chache_elemnt type ka hai jisme data url ye sb tha

				pos++;
			}

			//us socket pe itni bytes send kr denge ham jo client request kiya hga jo v socket se
			send(socket,response,MAX_BYTES,0);
		}
		printf("Data retrived from the Cache\n\n");
		printf("%s\n\n",response);
		// close(socketNew);
		// sem_post(&seamaphore);
		// return NULL;
	}


	//agr temp==null hi rha agr to iska mtlb ki cache me present nhi tha
	//to phle ap dekhoge ki client se request succesfully aa chuki hai ya nhi
	//agr aa chuki hai to phle dekho ki bytes_send_client>0 positive hona chaiye---agr positive hai tbhi ham server ko request krnge

	else if(bytes_send_client > 0)
	{
		len = strlen(buffer); 
		//Parsing the request
		//dekho ab cache me nhi hai to ab parsed request use krne pdega ab to iske liye lirary iuse krna pdega hame
		//uske header ye sb nikalne k liye yar
		//ye ek struct return krta hai...jisme method...protocol ye sb rehta hai
		ParsedRequest* request = ParsedRequest_create();
		
        //ParsedRequest_parse returns 0 on success and -1 on failure.On success it stores parsed request in
        // the request

		//buffer me jo v aa rkha hai usko parse krke request me dal do
		if (ParsedRequest_parse(request, buffer, len) < 0) 
		{
			//agr nhi hua parsing t eror return kr do 
		   	printf("Parsing failed\n");
		}
		else
		{	
			//agr ho gya hai to ise handle krna hga
			//wahi ki buffer khali kr lo
			bzero(buffer, MAX_BYTES);

			//ab mai string compare krunga....ham sirf GET request handle krnge in this---
			//agr dono ka value equeal hai jo parse krke method hai uska get and "get " string equal hai ....
			//dont get confuse by !-- as equal pe 0 return krta hai equal pe..isliey !0==1 to if me ghusega
			if(!strcmp(request->method,"GET"))							
			{
                //phle dekh lo ki jo v hao request ka ki sab chij ache se parse ho chuka hai na
				if( request->host && request->path && (checkHTTPversion(request->version) == 1) )
				{

					//ye handle reeuqest upr bna rlha hai
					bytes_send_client = handle_request(socket, request, tempReq);		// Handle GET request
					if(bytes_send_client == -1)
					{	
						//agr handle_request krne k bad v server se kuch nhi ata to send error mssg
						//not found
						//end server v  ny de paya request to
						sendErrorMessage(socket, 500);
					}

				}
				else
					sendErrorMessage(socket, 500);			// 500 Internal Error

			}
            else
            {
                printf("This code doesn't support any method other than GET\n");
            }
    
		}
        //freeing up the request pointer
		ParsedRequest_destroy(request);

	}
	else if( bytes_send_client < 0)
	{
		perror("Error in receiving from client.\n");
	}
	 //agr client se hi request ny aa rhi mtlb disconnected hai

	else if(bytes_send_client == 0)
	{
		printf("Client disconnected!\n");
	}
//jo v socket bnaya hia close kro
//socket shutdown kr do read write...(RDWR)
	shutdown(socket, SHUT_RDWR);
	close(socket);
	//buffer dynamically allocate hua to usko v free kr do
	free(buffer);
	//sem_post is same as sem_signal() value++ kr dega


// sem_getvalue(&semaphore, &p);
// printf("Semaphore value before post: %d\n", p);
// 	sem_post(&semaphore);	
	
// 	sem_getvalue(&semaphore,&p);
// 	printf("Semaphore post value:%d\n",p);

//     fflush(stdout);
    // Before returning, post to semaphore
	// if (sem_post(semaphore) != 0) {
    //     perror("sem_post failed");
    // }
    // sem_getvalue(semaphore, &p);
    // printf("Thread %lu: Semaphore value after post: %d\n", tid, p);
    // fflush(stdout);

	// free(tempReq);
	// return NULL;

	sem_post(&seamaphore);	
	
	sem_getvalue(&seamaphore,&p);
	printf("Semaphore post value:%d\n",p);
	free(tempReq);
	return NULL;

	
}





//..............................................MAIN CODE..............................................









int main(int argc, char * argv[]) {

//dekh bhai ye sab concept na networking ka socket programing ka hai--ki seerver anc lcient k liye na socket phle kholna phir connection ye sb estalblish tab hota hai

	//jo v client apke sath socket kholna chahta hai uska socket id k liye varibale and uske adress length ye sb store k liye client_len---ye sb search net pe krne se milega

	int client_socketId, client_len; // client_socketId == to store the client socket id

	//2 struck bna leta hun client ans socket address ka
	//jo server pe mai request mar rha hun and jo client se request aa rhi hai mujhe vo...to client k liye upr me to socket id le liya maine


	struct sockaddr_in server_addr, client_addr; // Address of client and server to be assigned

	//jo upr me semaphore bnaya tha usko intiliaze kr dete hai max_client jitna v banaya tha hamne jo upr define kr rkha hai
	//minimum value hai 0 and maximum it can go upto MAX_CLIENT
	//&semaphore...adress btana padta hai....ye syntax hai iska....
// 	if (sem_init(&semaphore, 0, MAX_CLIENTS) != 0) {
// 		perror("Failed to initialize semaphore");
// 		exit(1);
// 	}
//     sem_init(&semaphore,0,MAX_CLIENTS); // Initializing seamaphore and lock
// 	int initial_value;
// sem_getvalue(&semaphore, &initial_value);
// printf("Semaphore initialized with value: %d\n", initial_value);
// fflush(stdout);
	// if (sem_init(&seamaphore, 0, MAX_CLIENTS) != 0) {
	// 	perror("Semaphore initialization failed");
	// 	exit(1);
	// }

// sem_t semaphore;
// if (sem_init(&semaphore, 0, MAX_CLIENTS) != 0) {
//     perror("Failed to initialize semaphore");
//     return 1;
// }

// // Print initial value to verify initialization
// int initial_value;
// sem_getvalue(&semaphore, &initial_value);
// printf("Semaphore initialized with value: %d\n", initial_value);
// fflush(stdout);

// In main(), replace the current semaphore initialization
// Create a named semaphore
//sem_unlink("/my_semaphore");
// semaphore = sem_open("/my_semaphore", O_CREAT, 0644, MAX_CLIENTS);
// if (semaphore == SEM_FAILED) {
//     perror("sem_open failed");
//     return 1;
// }

// // Print initial value to verify initialization
// int initial_value;
// sem_getvalue(semaphore, &initial_value);
// printf("Semaphore initialized with value: %d\n", initial_value);
// fflush(stdout);
// Then create and initialize the semaphore
// semaphore = sem_open("/my_semaphore", O_CREAT , 0644, MAX_CLIENTS);
// if (semaphore == SEM_FAILED) {
//     perror("sem_open failed");
//     return 1;
// }

// // Verify initialization
// int initial_value;
// if (sem_getvalue(semaphore, &initial_value) == 0) {
//     printf("Initial semaphore value: %d\n", initial_value);
//     if (initial_value != MAX_CLIENTS) {
//         // Try to set the initial value explicitly
//         while (initial_value < MAX_CLIENTS) {
//             sem_post(semaphore);
//             sem_getvalue(semaphore, &initial_value);
//         }
//         printf("Adjusted semaphore value to: %d\n", initial_value);
//     }
// }
// fflush(stdout);

sem_init(&seamaphore,0,MAX_CLIENTS); // Initializing seamaphore and lock


	//ab mutex ko v intit i.e intialize kr dete hai...and intialize by null....ny to c language me by default sari variable me value garbage rehti hai
    pthread_mutex_init(&lock,NULL); // Initializing lock for cache
    
//during compilation terminal me jab argument pass krnge---
//agr kisi ko 8080 port pe nhi chalan hai...9090 oe chalana chata hai then vo port de skta hai

	if(argc == 2)        //checking whether two arguments are received or not
	{
		//to agr mai coomand prompt me ./port 9090 likhu to 9090 pe intialize kr dega
		port_number = atoi(argv[1]); //jo v command line pe dete ho vo string me rhta hai isliye usko integer me badal liya by ATOI() se
	}
	else
	{
		printf("Too few arguments\n");  //wrna few argument
		exit(1);//and exit kr jana
	}


	//agr band nhi hua to ye orint kr denge ham

	printf("Setting Proxy Server Port : %d\n",port_number);

    //creating the proxy socket
	//jo hamne upr proxy_web_serve ka socketid banaya tha to ye proxyserver ka v socket open krna pdega na ji
	//proxy ka to ek hi socket rehta hai ...jispe har ek client request krega..main proxy 
//like google.com client1 ne kiya request to phle main socket i.e proxy pe jiga...and then waha se ek naya thread generate hga jo is google.com ko serve krega
//isliye main proxy banane k liye uska socket() bana rhe hai
//IPV4  use kr rhe hai ham---TCP(sock_stream)


	proxy_socketId = socket(AF_INET, SOCK_STREAM, 0);


	//agr koi v socket create ny kr pate hai to uski return vlue hoti hai -ve
	//isliye agr create ny kr piaga to error raise kr denge and exit
	if( proxy_socketId < 0)
	{
		perror("Failed to create socket.\n");
		exit(1);
	}
//agr socket ban jata hai....abhi upr maine bataya ki ye proxy main wala hoga...main wala common hoga har koi k liye uska socket bana rhe the ham
//to yha pe jitna v client hoga sab ayega hi..iske bad se alg alg thread create hga...
//to chuki ye main wala bar bar reuse ho rha hai na..jab jab naya client aa rha hai to
//to resue krne k liye v option set krne pdenge...i.e setsockopt()

	int reuse =1;
	//aise nhi bolne dena hai...like google aya to and again fb aa gya to aise nhi bolne dena hai ki socket is already in use..vo sb ny..isliye ye niche wala chij likh rkhe hai
	//agr hga to error raise krnge ham
	if (setsockopt(proxy_socketId, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) < 0) 
        perror("setsockopt(SO_REUSEADDR) failed\n");


		//in c langugae jo v struct ye sab bnate ho initially garbage value store krta hai
		//isliye us garbage value ko clean krna pdta hai
		//jo v socket ka server adress bnaya tha uska adress de rha hun BZERO ko ye sb me 0 put kr dega...mtlb clean kr dega
	bzero((char*)&server_addr, sizeof(server_addr));  
	//AF_INET na IPV$ network hai dhyan se
	server_addr.sin_family = AF_INET;

	//htons jo hai na jo internet ko smajh me ata hai n numbers usme convert kr deta hai
	server_addr.sin_port = htons(port_number); // Assigning port to the Proxy

	//sin family me hamne jakr in_addr..hmane bola ki is socket pe pe jis server se communicate krna chahte ho uspe koi v adress ap define kr do for this time
	server_addr.sin_addr.s_addr = INADDR_ANY; // Any available adress assigned

    // Binding the socket
	//bind krna jruri hota hai as jab v apko socket ko open krna hota hai then we need to bind it
	//agr bind nhi hua it mean ki port availaible nhi hai...port nhi hai tbhi bind nhi hua...and it will return<0
	if( bind(proxy_socketId, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0 )
	{
		perror("Port is not free\n");
		exit(1);
	}
	printf("Binding on port: %d\n",port_number);

    // Proxy socket listening to the requests
	//apna jo proxy server ka jo adress socket hai vo listen krna suru kr dega..
	int listen_status = listen(proxy_socketId, MAX_CLIENTS);


	//eror checking if <0 then error listening
	if(listen_status < 0 )
	{
		perror("Error while Listening !\n");
		exit(1);
	}


	//ab aa jate hai apan iterator define krne pe ki kitne client coneect hue and kitne ny...to apan uske liye coonected_socketId array ka use---to usko v rkhna chahoge 
	int i = 0; // Iterator for thread_id (tid) and Accepted Client_Socket for each thread
	int Connected_socketId[MAX_CLIENTS];   // This array stores socket descriptors of connected clients...to obvious kitne client hai..max_cliet jo define kr rkha hai...to use krna pdega utne size ka array 

    // Infinite Loop for accepting connections
	//ek inifinite loop chalaoge 
	//yha pe ham client se jo main proxy hai uspe connection accept krwa rhe hain
	//is loop ke andr client jb tk ate rehta hai...and hr client k liye uske new thread dena hga 
	while(1)
	{

		//isme hemsha app suru me ap apne client ka jo adress hai usko phle to clean kroge by using bzero
		bzero((char*)&client_addr, sizeof(client_addr));			// Clears struct client_addr
		client_len = sizeof(client_addr); 

        // Accepting the connections
		//cleint ke socket se accept kr rhe honge ham
		client_socketId = accept(proxy_socketId, (struct sockaddr*)&client_addr,(socklen_t*)&client_len);	// Accepts connection
		if(client_socketId < 0)
		{
			fprintf(stderr, "Error in Accepting connection !\n");
			exit(1);
		}
		else{
			//jo client ke sath ap connect hue ho usko array me dal doge vo wla id at i-th index
			Connected_socketId[i] = client_socketId; // Storing accepted client into array
		}

		// Getting IP address and port number of client
		struct sockaddr_in* client_pt = (struct sockaddr_in*)&client_addr;
		struct in_addr ip_addr = client_pt->sin_addr;
		char str[INET_ADDRSTRLEN];										// INET_ADDRSTRLEN: Default ip address size
		inet_ntop( AF_INET, &ip_addr, str, INET_ADDRSTRLEN );
		printf("\nClient is connected with port number: %d and ip address: %s \n",ntohs(client_addr.sin_port), str);
		//printf("Socket values of index %d in main function is %d\n",i, client_socketId);


		//ab pthread create kr lo...as sara client ka socket open ho gya hai...connection accept ho gya hai client se...
		//apne main proxy ke socket jo tha uspe client ka  connection accept kr liya hai 
		//ab jo client ye ,ain proxy sath socket open kiya hai..uske liye har naye thread me socket dena pdega
		 //taki baki wale client baki thread pe aa sake
		 //thread_fn ye ek function hai--ki ye ek naya thread bna do is client k liye pr usme ye function exceute kro which we will define further
		pthread_create(&tid[i],NULL,thread_fn, (void*)&Connected_socketId[i]); // Creating a thread for each client accepted
		i++; 
	}
	// This tries to join ALL possible threads (MAX_CLIENTS) even if you haven't created that many. Instead, you should track how many threads you've actually created:
	// for (int i = 0; i < MAX_CLIENTS; i++) {
	// 	pthread_join(tid[i], NULL);
	// }
	// In main(), modify the join loop
for (int j = 0; j < i; j++) {  // Use 'i' as it tracks number of threads created
    pthread_join(tid[j], NULL);
}

	//jab v loop khtm to jo v apne pointer bnate ho uski memory khali kr do and return 0;
	//ab upr dekho thread_fn likh lete hai jo aise to kuch return ny krta hai pr lets make it
	close(proxy_socketId);									// Close socket
 	return 0;
}









cache_element* find(char* url){

// Checks for url in the cache if found returns pointer to the respective cache element or else returns NULL
    cache_element* site=NULL;
	//sem_wait(&cache_lock);

	//ye hmko ake lock de dega
    int temp_lock_val = pthread_mutex_lock(&lock);	

	//chuki ap cache pe kaam kr rhe ho to lock acquire to krna pdega na

	printf("Remove Cache Lock Acquired %d\n",temp_lock_val); 
    if(head!=NULL){
        site = head;
        while (site!=NULL)
        {
			//site is pointed to head
			//now us head i.e site->url and original URL agr match krta hai to url found print krnge
			
            if(!strcmp(site->url,url)){
				printf("LRU Time Track Before : %ld", site->lru_time_track);
                printf("\nurl found\n");
				// Updating the time_track
				//ye avi avi use hua hai isly iska time update kr diya
				//ye time wala v library na hai already hai .h header file me
				site->lru_time_track = time(NULL);
				printf("LRU Time Track After : %ld", site->lru_time_track);
				break;
            }
            site=site->next;
        }       
    }
	else {
		//agr head==null pe rha to add hi nhi kr paiga na...isliye we will print it not found
    printf("url not found\n");
	}
	//sem_post(&cache_lock);
	//phir remove the lock...i.e lock jo acquire kiya tha usko hata do

    temp_lock_val = pthread_mutex_unlock(&lock);
	printf("Remove Cache Lock Unlocked %d\n",temp_lock_val); 
    return site;
}




//-------------------------------------------remove---------------------------

void remove_cache_element(){

	//node ko delete krne k liye previous ye sb node krna hota hta hai.....DSA ka part hai ye
    // If cache is not empty searches for the node which has the least lru_time_track and deletes it
    cache_element * p ;  	// Cache_element Pointer (Prev. Pointer)
	cache_element * q ;		// Cache_element Pointer (Next Pointer)
	cache_element * temp;	// Cache element to remove
    //sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&lock);
	printf("Remove Cache Lock Acquired %d\n",temp_lock_val); 
	if( head != NULL) { // Cache != empty
		for (q = head, p = head, temp =head ; q -> next != NULL; 
			q = q -> next) { // Iterate through entire cache and search for oldest time track
			if(( (q -> next) -> lru_time_track) < (temp -> lru_time_track)) {
				temp = q -> next;
				p = q;
			}
		}

		//agr temp==head pe hi reh gya then simple head hi bdl do na---
		if(temp == head) { 
			head = head -> next; /*Handle the base case*/
		} else {
			p->next = temp->next;	
		}

		//delete kra hai...isliye use cache_size jo v hai minus kr dnge
		cache_size = cache_size - (temp -> len) - sizeof(cache_element) - strlen(temp -> url) - 1;     //updating the cache size
		free(temp->data);     		
		free(temp->url); // Free the removed element 
		free(temp);
	} 
	//sem_post(&cache_lock);
    temp_lock_val = pthread_mutex_unlock(&lock);
	printf("Remove Cache Lock Unlocked %d\n",temp_lock_val); 
}






//--------------------adding to the cache------------------------
int add_cache_element(char* data,int size,char* url){
    // Adds element to the cache
	// sem_wait(&cache_lock);

	//lock acquire kr diye chache pe
    int temp_lock_val = pthread_mutex_lock(&lock);
	printf("Add Cache Lock Acquired %d\n", temp_lock_val);

    int element_size=size+1+strlen(url)+sizeof(cache_element); // Size of the new element which will be added to the cache
    if(element_size>MAX_ELEMENT_SIZE){

		//max element size upr define kr diye hai
    

		//sem_post(&cache_lock);
        // If element size is greater than MAX_ELEMENT_SIZE we don't add the element to the cache
		//isliye unlock hatana hoga
        temp_lock_val = pthread_mutex_unlock(&lock);
		printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
		// free(data);
		// printf("--\n");
		// free(url);
        return 0;
    }
    else
    {   while(cache_size+element_size>MAX_SIZE){

		//max size upr define kiye hai total maxsize...agr isse badi hai to tab tak remove krte jaw...till jb tk under max_size na aa jaye
            // We keep removing elements from cache until we get enough space to add the element
            remove_cache_element();
        }
        cache_element* element = (cache_element*) malloc(sizeof(cache_element)); // Allocating memory for the new cache element
        element->data= (char*)malloc(size+1); // Allocating memory for the response to be stored in the cache element
		strcpy(element->data,data); 
        element -> url = (char*)malloc(1+( strlen( url )*sizeof(char)  )); // Allocating memory for the request to be stored in the cache element (as a key)
		strcpy( element -> url, url );
		element->lru_time_track=time(NULL);    // Updating the time_track
        element->next=head; 
        element->len=size;
        head=element;
        cache_size+=element_size;
        temp_lock_val = pthread_mutex_unlock(&lock);
		printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
		//sem_post(&cache_lock);
		// free(data);
		// printf("--\n");
		// free(url);
        return 1;
    }
    return 0;
}
