#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static void die(const char *s) { perror(s); exit(1); }
static uint64_t now_ns(void) { struct timespec t; if (clock_gettime(CLOCK_MONOTONIC, &t)) die("clock"); return (uint64_t)t.tv_sec*1000000000ULL+t.tv_nsec; }
static void transfer(int fd, char *buf, size_t n, int sending) {
    size_t p=0;
    while (p<n) {
        ssize_t r = sending ? send(fd, buf+p, n-p, MSG_NOSIGNAL) : recv(fd, buf+p, n-p, 0);
        if (r<0 && (errno==EINTR || errno==EAGAIN || errno==EWOULDBLOCK)) continue;
        if (r<=0) die("transfer");
        p+=(size_t)r;
    }
}
static int cmp(const void *a, const void *b) { uint64_t x=*(const uint64_t*)a,y=*(const uint64_t*)b; return (x>y)-(x<y); }
static double root(double x) { if (x<=0) return 0; double v=x>1?x:1; for(int i=0;i<64;i++) v=(v+x/v)/2; return v; }
int main(int argc, char **argv) {
    if (argc!=8 || (strcmp(argv[1],"server") && strcmp(argv[1],"client"))) return 2;
    int server=!strcmp(argv[1],"server");
    char *end; long port=strtol(argv[3],&end,10); if (*end || port<1 || port>65535) return 2;
    long size=strtol(argv[5],&end,10); if (*end || size<1 || size>65536) return 2;
    long iterations=strtol(argv[6],&end,10); if (*end || iterations<1 || iterations>20000) return 2;
    long warmup=strtol(argv[7],&end,10); if (*end || warmup<0 || warmup>2000) return 2;
    if (strcmp(argv[4],"block") && strcmp(argv[4],"spin") && strcmp(argv[4],"busy_poll")) return 2;
    struct sockaddr_in addr={.sin_family=AF_INET,.sin_port=htons((uint16_t)port)};
    if (inet_pton(AF_INET,argv[2],&addr.sin_addr)!=1) return 2;
    int fd=socket(AF_INET,SOCK_STREAM,0); if(fd<0) die("socket");
    if(server) {
        int one=1; if(setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(one))) die("reuseaddr");
        if(bind(fd,(struct sockaddr*)&addr,sizeof(addr)) || listen(fd,1)) die("listen");
        int c=accept(fd,NULL,NULL); if(c<0) die("accept"); close(fd); fd=c;
    } else if(connect(fd,(struct sockaddr*)&addr,sizeof(addr))) die("connect");
    int one=1; if(setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one))) die("nodelay");
    int busy_result=0,busy_errno=0;
    if(!strcmp(argv[4],"busy_poll")) { int v=50; busy_result=setsockopt(fd,SOL_SOCKET,SO_BUSY_POLL,&v,sizeof(v)); if(busy_result) busy_errno=errno; }
    if(!strcmp(argv[4],"spin")) { int f=fcntl(fd,F_GETFL); if(f<0 || fcntl(fd,F_SETFL,f|O_NONBLOCK)) die("fcntl"); }
    char *buf=calloc((size_t)size,1); uint64_t *samples=calloc((size_t)iterations,sizeof(*samples));
    if(!buf || !samples) die("calloc");
    uint64_t histogram[40]={0}; double sum=0, sumsq=0;
    for(long i=0;i<iterations+warmup;i++) {
        uint64_t start=server?0:now_ns();
        if(server) { transfer(fd,buf,(size_t)size,0); transfer(fd,buf,(size_t)size,1); }
        else { transfer(fd,buf,(size_t)size,1); transfer(fd,buf,(size_t)size,0); }
        if(!server && i>=warmup) { uint64_t v=now_ns()-start; samples[i-warmup]=v; sum+=(double)v; sumsq+=(double)v*(double)v; unsigned b=0; uint64_t x=v; while(x>1 && b<39) { x>>=1; b++; } histogram[b]++; }
    }
    if(!server) {
        qsort(samples,(size_t)iterations,sizeof(*samples),cmp); double mean=sum/iterations, variance=sumsq/iterations-mean*mean;
        printf("{\"size\":%ld,\"iterations\":%ld,\"warmup\":%ld,\"min_ns\":%llu,\"p50_ns\":%llu,\"p90_ns\":%llu,\"p99_ns\":%llu,\"p999_ns\":%llu,\"max_ns\":%llu,\"mean_ns\":%.3f,\"stdev_ns\":%.3f,\"histogram_log2_ns\":[",size,iterations,warmup,(unsigned long long)samples[0],(unsigned long long)samples[(iterations-1)*50/100],(unsigned long long)samples[(iterations-1)*90/100],(unsigned long long)samples[(iterations-1)*99/100],(unsigned long long)samples[(iterations-1)*999/1000],(unsigned long long)samples[iterations-1],mean,root(variance));
        for(int i=0;i<40;i++) printf("%s%llu",i?",":"",(unsigned long long)histogram[i]);
        printf("],\"setsockopt\":{\"requested\":%s,\"result\":%d,\"errno\":%d}}\n",!strcmp(argv[4],"busy_poll")?"true":"false",busy_result,busy_errno);
    }
    free(samples); free(buf); close(fd); return 0;
}
