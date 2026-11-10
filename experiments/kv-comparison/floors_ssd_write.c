#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

static unsigned long long now_ns(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &t)) { perror("clock_gettime"); exit(1); }
    return (unsigned long long)t.tv_sec * 1000000000ULL + t.tv_nsec;
}
struct row { unsigned long long write_ns, sync_ns, total_ns, offset; };
int main(int argc, char **argv) {
    if (argc != 11) return 2;
    const char *root = argv[1], *name = argv[2], *log = argv[3], *mode = argv[4];
    unsigned long block = strtoul(argv[5], NULL, 10), region = strtoul(argv[6], NULL, 10);
    unsigned long seconds = strtoul(argv[7], NULL, 10), cap = strtoul(argv[8], NULL, 10);
    unsigned long long expected_dev = strtoull(argv[9], NULL, 10), expected_ino = strtoull(argv[10], NULL, 10);
    int append = !strcmp(mode, "append_pwrite") || !strcmp(mode, "append_mmap") || !strcmp(mode, "append_mmap_fsync");
    int mapped = !strcmp(mode, "append_mmap") || !strcmp(mode, "append_mmap_fsync");
    if (strchr(name, '/') || !*name || !strcmp(name, ".") || !strcmp(name, "..") ||
        (!append && strcmp(mode, "buffered") && strcmp(mode, "fdatasync") && strcmp(mode, "direct")) ||
        (block != 1024 && block != 8192 && block != 65536) || region != 536870912 || seconds != 6 ||
        cap != (append ? 4096UL : 100000UL)) return 2;
    int dir = open(root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (dir < 0) { perror("scratch root"); return 1; }
    int fd = openat(dir, name, (mapped ? O_RDWR : O_WRONLY) | O_NOFOLLOW | O_CLOEXEC | (!strcmp(mode, "direct") ? O_DIRECT : 0));
    int open_errno = errno;
    close(dir);
    struct stat st;
    if (fd < 0) { fprintf(stderr, "scratch open errno=%d: %s\n", open_errno, strerror(open_errno)); return 1; }
    if (fstat(fd, &st) || !S_ISREG(st.st_mode) || (unsigned long long)st.st_size != region) { perror("scratch"); return 1; }
    if ((unsigned long long)st.st_dev != expected_dev || (unsigned long long)st.st_ino != expected_ino) {
        fprintf(stderr, "scratch inode mismatch: expected %llu:%llu observed %llu:%llu\n",
                expected_dev, expected_ino, (unsigned long long)st.st_dev, (unsigned long long)st.st_ino);
        return 3;
    }
    if (append && st.st_blocks != 0) { fprintf(stderr, "scratch is not sparse\n"); return 1; }
    int logfd = open(log, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (logfd < 0) { perror("log"); return 1; }
    FILE *out = fdopen(logfd, "w");
    void *buf = NULL;
    if (!out || posix_memalign(&buf, 4096, block)) return 1;
    struct row *rows = calloc(cap, sizeof(*rows));
    if (!rows) return 1;
    memset(buf, 0xa5, block);
    void *mapping = NULL;
    if (mapped) {
        mapping = mmap(NULL, region, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, 0);
        if (mapping == MAP_FAILED) { perror("mmap"); return 1; }
    }
    unsigned long long start = now_ns(), end = start + seconds * 1000000000ULL, count = 0, kept = 0;
    unsigned long long offset = 0;
    while (append ? count < 512 + cap : now_ns() < end) {
        unsigned long long a = now_ns();
        if (mapped) memcpy((char *)mapping + offset, buf, block);
        else if (pwrite(fd, buf, block, offset) != (ssize_t)block) { fprintf(stderr, "pwrite errno=%d: %s\n", errno, strerror(errno)); return 1; }
        unsigned long long b = now_ns(), c = b;
        if (!strcmp(mode, "fdatasync") || !strcmp(mode, "append_pwrite") || !strcmp(mode, "append_mmap")) { if (fdatasync(fd)) { perror("fdatasync"); return 1; } c = now_ns(); }
        if (!strcmp(mode, "append_mmap_fsync")) { if (fsync(fd)) { perror("fsync"); return 1; } c = now_ns(); }
        if ((!append || count >= 512) && kept < cap) rows[kept++] = (struct row){b-a, c-b, c-a, offset};
        count++;
        offset += block;
        if (!append && offset + block > region) offset = 0;
        if (append && count == 512) start = now_ns();
    }
    unsigned long long elapsed = now_ns() - start;
    if (append) count -= 512;
    if (mapped && munmap(mapping, region)) { perror("munmap"); return 1; }
    for (unsigned long long i = 0; i < kept; i++)
        if (fprintf(out, "%llu,%llu,%llu,%llu\n", rows[i].write_ns, rows[i].sync_ns, rows[i].total_ns, rows[i].offset) < 0) return 1;
    if (fclose(out) || close(fd)) return 1;
    free(rows);
    free(buf);
    printf("{\"total_operations\":%llu,\"logged_operations\":%llu,\"elapsed_ns\":%llu}\n", count, kept, elapsed);
    return 0;
}
