/* SPDX-License-Identifier: Apache-2.0
 * Exercise the actual pinned SPDK RPC callbacks, not a copied implementation.
 * Compile with SPDK_NVMF_RPC_SOURCE set to the admitted nvmf_rpc.c path,
 * function/data sections and --gc-sections. Registration is unnecessary here.
 */
#include "spdk/stdinc.h"
#include "spdk/rpc.h"
#undef SPDK_RPC_REGISTER
#define SPDK_RPC_REGISTER(...)
#include SPDK_NVMF_RPC_SOURCE

static unsigned removes, resumes, successes, errors;
static int remove_result, resume_result, resume_status, first_error;

void
spdk_jsonrpc_send_bool_response(struct spdk_jsonrpc_request *request, bool value)
{
	assert(request == (void *)1 && value);
	successes++;
}

void
spdk_jsonrpc_send_error_response(struct spdk_jsonrpc_request *request, int code, const char *message)
{
	assert(request == (void *)1 && message != NULL);
	if (errors++ == 0) {
		first_error = code;
	}
}

void
spdk_jsonrpc_send_error_response_fmt(struct spdk_jsonrpc_request *request, int code,
				     const char *format, ...)
{
	spdk_jsonrpc_send_error_response(request, code, format);
}

void
spdk_log(enum spdk_log_level level, const char *file, const int line, const char *function,
	 const char *format, ...)
{
	(void)level;
	(void)file;
	(void)line;
	(void)function;
	(void)format;
}

int
spdk_nvmf_subsystem_remove_ns(struct spdk_nvmf_subsystem *subsystem, uint32_t nsid)
{
	(void)subsystem;
	assert(nsid == 7);
	removes++;
	return remove_result;
}

int
spdk_nvmf_subsystem_resume(struct spdk_nvmf_subsystem *subsystem,
			 spdk_nvmf_subsystem_state_change_done callback, void *argument)
{
	resumes++;
	if (resume_result == 0) {
		/* Immediate completion also checks callback-context ownership. */
		callback(subsystem, argument, resume_status);
	}
	return resume_result;
}

static struct nvmf_rpc_remove_ns_ctx *
context(void)
{
	struct nvmf_rpc_remove_ns_ctx *ctx = calloc(1, sizeof(*ctx));
	assert(ctx != NULL);
	ctx->nqn = strdup("nqn.2026-11.org.tidehunter:drain-status-unit");
	assert(ctx->nqn != NULL);
	ctx->request = (void *)1;
	ctx->nsid = 7;
	return ctx;
}

static void
reset(void)
{
	removes = resumes = successes = errors = 0;
	remove_result = resume_result = resume_status = first_error = 0;
}

int
main(void)
{
	reset();
	nvmf_rpc_remove_ns_paused(NULL, context(), -EIO);
	assert(removes == 0 && resumes == 0 && successes == 0 && errors == 1);
	assert(first_error == SPDK_JSONRPC_ERROR_INTERNAL_ERROR);

	reset();
	resume_status = -EIO;
	nvmf_rpc_remove_ns_paused(NULL, context(), 0);
	assert(removes == 1 && resumes == 1 && successes == 0 && errors == 1);
	assert(first_error == SPDK_JSONRPC_ERROR_INTERNAL_ERROR);

	reset();
	remove_result = -EINVAL;
	resume_status = -EIO;
	nvmf_rpc_remove_ns_paused(NULL, context(), 0);
	assert(removes == 1 && resumes == 1 && successes == 0 && errors == 1);
	assert(first_error == SPDK_JSONRPC_ERROR_INVALID_PARAMS);

	reset();
	resume_result = -EIO;
	nvmf_rpc_remove_ns_paused(NULL, context(), 0);
	assert(removes == 1 && resumes == 1 && successes == 0 && errors == 1);

	reset();
	nvmf_rpc_remove_ns_paused(NULL, context(), 0);
	assert(removes == 1 && resumes == 1 && successes == 1 && errors == 0);
	puts("SPDK namespace removal: five success/error transition cases passed");
	return 0;
}
