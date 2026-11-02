/* ABI boundary for Valkey 9.0.7 bundled libvalkey (BSD-3-Clause), commit
 * fe8d406c0b5a64ec897f6455e4de684629883764. All layout access is compiled
 * against that checkout's headers; commands and RDMA are upstream code. */
#define _POSIX_C_SOURCE 200809L
#include <valkey/valkey.h>
#include <valkey/rdma.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>
#include <time.h>

/* Capture errno before Rust or another library call can overwrite it. The two
 * RDMA IO strings are emitted specifically on upstream poll/deadline expiry. */
static int kv_timed_out(const valkeyContext *c, int rdma, int saved_errno) {
    if (!c) return 0;
    if (c->err == VALKEY_ERR_TIMEOUT) return 1;
    if (c->err != VALKEY_ERR_IO) return 0;
    return saved_errno == ETIMEDOUT ||
           (rdma && (!strcmp(c->errstr, "RDMA: IO timeout") ||
                     !strcmp(c->errstr, "Resource temporarily unavailable")));
}

int kv_rdma_init(void) { return valkeyInitiateRdma(); }
valkeyContext *kv_rdma_connect(const char *host, int port, uint64_t ms, int rdma, int *timed_out) {
    struct timeval tv = { (long)(ms / 1000), (long)((ms % 1000) * 1000) };
    valkeyOptions opts = {0};
    if (rdma) { VALKEY_OPTIONS_SET_RDMA(&opts, host, port); }
    else { VALKEY_OPTIONS_SET_TCP(&opts, host, port); }
    opts.connect_timeout = &tv;
    opts.command_timeout = &tv;
    errno = 0;
    valkeyContext *c = valkeyConnectWithOptions(&opts);
    int saved_errno = errno;
    *timed_out = kv_timed_out(c, rdma, saved_errno);
    return c;
}
int kv_context_error(const valkeyContext *c) { return c ? c->err : -1; }
const char *kv_context_error_string(const valkeyContext *c) { return c ? c->errstr : "libvalkey allocation failed"; }
void kv_context_free(valkeyContext *c) { if (c) valkeyFree(c); }
int kv_append(valkeyContext *c, int n, const char **args, const size_t *lengths, int rdma, int *timed_out) {
    errno = 0;
    int result = valkeyAppendCommandArgv(c, n, args, lengths);
    int saved_errno = errno;
    *timed_out = result != VALKEY_OK && kv_timed_out(c, rdma, saved_errno);
    return result;
}
static int kv_set_deadline_timeout(valkeyContext *c, int64_t deadline_ns, int *timed_out) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return VALKEY_ERR;
    int64_t remaining = deadline_ns - ((int64_t)now.tv_sec * 1000000000 + now.tv_nsec);
    if (remaining <= 0) { *timed_out = 1; return VALKEY_ERR; }
    uint64_t ms = (uint64_t)(remaining + 999999) / 1000000;
    struct timeval tv = { (long)(ms / 1000), (long)((ms % 1000) * 1000) };
    return valkeySetTimeout(c, tv);
}
int kv_set_timeout(valkeyContext *c, uint64_t ms) {
    struct timeval tv = { (long)(ms / 1000), (long)((ms % 1000) * 1000) };
    return valkeySetTimeout(c, tv);
}
/* valkeyGetReply resets its timeout on every buffer read. Use the same public
 * upstream buffer/parser APIs with a shrinking timeout to bound slow replies. */
valkeyReply *kv_get_reply(valkeyContext *c, int rdma, uint64_t remaining_ms, int *timed_out) {
    struct timespec now;
    void *reply = NULL;
    int done = 0;
    *timed_out = 0;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return NULL;
    int64_t deadline = (int64_t)now.tv_sec * 1000000000 + now.tv_nsec + (int64_t)remaining_ms * 1000000;
    if (valkeyGetReplyFromReader(c, &reply) != VALKEY_OK) goto error;
    if (reply) return reply;
    do {
        if (kv_set_deadline_timeout(c, deadline, timed_out) != VALKEY_OK) goto error;
        errno = 0;
        if (valkeyBufferWrite(c, &done) != VALKEY_OK) {
            if (!rdma && c->err == VALKEY_ERR_IO && (errno == EAGAIN || errno == EWOULDBLOCK)) *timed_out = 1;
            goto error;
        }
    } while (!done);
    do {
        if (kv_set_deadline_timeout(c, deadline, timed_out) != VALKEY_OK) goto error;
        errno = 0;
        if (valkeyBufferRead(c) != VALKEY_OK) {
            if (!rdma && c->err == VALKEY_ERR_IO && (errno == EAGAIN || errno == EWOULDBLOCK)) *timed_out = 1;
            goto error;
        }
        if (valkeyGetReplyFromReader(c, &reply) != VALKEY_OK) goto error;
    } while (!reply);
    return reply;
error:
    if (!*timed_out) *timed_out = kv_timed_out(c, rdma, errno);
    return NULL;
}
void kv_reply_free(valkeyReply *r) { if (r) freeReplyObject(r); }
int kv_reply_type(const valkeyReply *r) { return r->type; }
const char *kv_reply_data(const valkeyReply *r) { return r->str; }
size_t kv_reply_len(const valkeyReply *r) { return r->len; }
long long kv_reply_integer(const valkeyReply *r) { return r->integer; }
size_t kv_reply_count(const valkeyReply *r) { return r->elements; }
const valkeyReply *kv_reply_element(const valkeyReply *r, size_t i) { return i < r->elements ? r->element[i] : NULL; }
int kv_reply_string(void) { return VALKEY_REPLY_STRING; }
int kv_reply_array(void) { return VALKEY_REPLY_ARRAY; }
int kv_reply_integer_type(void) { return VALKEY_REPLY_INTEGER; }
int kv_reply_nil(void) { return VALKEY_REPLY_NIL; }
int kv_reply_status(void) { return VALKEY_REPLY_STATUS; }
int kv_reply_error(void) { return VALKEY_REPLY_ERROR; }
