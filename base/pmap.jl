# This file is a part of Julia. License is MIT: http://julialang.org/license

type BatchProcessingError <: ErrorException
    data
    ex
end

"""
    pgenerate([::WorkerPool], f, c...) -> iterator

Apply `f` to each element of `c` in parallel using available workers and tasks.

For multiple collection arguments, apply f elementwise.

Results are returned in order as they become available.

Note that `f` must be made available to all worker processes; see
[Code Availability and Loading Packages](:ref:`Code Availability
and Loading Packages <man-parallel-computing-code-availability>`)
for details.
"""
function pgenerate(p::WorkerPool, f, c; distributed=true, batch_size=1, on_error=nothing, retry=nothing)
    f_orig = f

    # Don't do remote calls if there are no workers.
    if (length(p) == 0) || (length(p) == 1 && fetch(p.channel) == myid())
        distributed = false
    end

    # Don't do batching if not doing remote calls.
    if !distributed
        batch_size = 1
    end

    # If not batching, do simple remote call.
    if batch_size == 1
        if distributed
            f = remote(p, f)
        end

        f = wrap_retry(f, retry)
        f = handle_on_error(f, on_error)
        return AsyncGenerator(f, c)
    else
        batches = batchsplit(c, min_batch_count = length(p) * 3,
                                max_batch_size = batch_size)

        # We need to ensure that if on_error is set, it is called for each element
        # in error, and that we return as many elements as the original list.
        # retry, if set, has to be called element wise and we will do a best-effort
        # to ensure that we do not call mapped function on the same element twice.
        # This guarantee is not possible in case of worker death / network errors, wherein
        # we will retry the entire batch on a new worker.
        f = handle_on_error(f, (x,e)->BatchProcessingError(x,e); capture_data=true)

        f = wrap_batch(f, p, on_error)

        results = flatten(AsyncGenerator(f, batches))

        # Handle all the ones in error in another pmap, with batch size set to 1
        if (on_error != nothing) || (retry != nothing)
            reprocess = []
            for (idx, v) in enumerate(results)
                if isa(v, BatchProcessingError)
                    push!(reprocess, (i,v))
                end
            end

            if length(reprocess) > 0
                errors = [x->x[2] for x in reprocess]
                data_in_error = [x.data for x in errors]
                processed_errors = pmap(p, f_orig, data_in_error; on_error=on_error, retry=retry)
                for (idx, v) in enumerate(processed_errors)
                    results[reprocess[idx][1]] = v
                end
            end
        end

        return results
    end
end

pgenerate(p::WorkerPool, f, c1, c...; kwargs...) = pgenerate(p, a->f(a...), zip(c1, c...); kwargs...)

pgenerate(f, c; kwargs...) = pgenerate(default_worker_pool(), f, c...; kwargs...)
pgenerate(f, c1, c...; kwargs...) = pgenerate(a->f(a...), zip(c1, c...); kwargs...)

function wrap_on_error(f, on_error; capture_data=false)
    if on_error != nothing
        return x -> begin
            try
                f(x)
            catch e
                if capture_data
                    on_error(x, e)
                else
                    on_error(e)
                end
            end
        end
    else
        return f
    end
end

wrap_retry(f, retryf) = (retryf != nothing ? retryf(f) : f)

function wrap_batch(f, p, on_error)
    f = asyncmap_batch(f)
    return batch -> begin
        try
            remotecall_fetch(f, p, batch)
        catch e
            if on_error != nothing
                return Any[BatchProcessingError(batch[i], e) for i in 1:length(batch)]
            else
                rethrow(e)
            end
        end
    end
end

asyncmap_batch(f) = batch -> asyncmap(f, batch)


"""
    pmap([::WorkerPool], f, c...; distributed=true, batch_size=1, on_error=nothing) -> collection

Transform collection `c` by applying `f` to each element using available
workers and tasks.

For multiple collection arguments, apply f elementwise.

Note that `f` must be made available to all worker processes; see
[Code Availability and Loading Packages](:ref:`Code Availability
and Loading Packages <man-parallel-computing-code-availability>`)
for details.

If a worker pool is not specified, all available workers, i.e., the default worker pool
is used.

By default, `pmap` distributes the computation over all specified workers. To use only the
local process and distribute over tasks, specifiy `distributed=false`

`pmap` can also use a mix of processes and tasks via the `batch_size` argument. For batch sizes
greater than 1, the collection is split into multiple batches, which are distributed across
workers. Each such batch is processed in parallel via tasks in each worker. The specified
`batch_size` is an upper limit, the actual size of batches may be smaller and is calculated
depending on the number of workers available and length of the collection.

Any error stops pmap from processing the remainder of the collection. To override this behavior
you can specify an error handling function via argument `on_error` which takes in a single argument, i.e.,
the exception. The function can stop the processing by rethrowing the error, or, to continue, return any value
which is then returned inline with the results to the caller.
"""
pmap(p::WorkerPool, f, c...; kwargs...) = collect(pgenerate(p, f, c...; kwargs...))


"""
    batchsplit(c; min_batch_count=1, max_batch_size=100) -> iterator

Split a collection into at least `min_batch_count` batches.

Equivalent to `partition(c, max_batch_size)` when `length(c) >> max_batch_size`.
"""
function batchsplit(c; min_batch_count=1, max_batch_size=100)
    if min_batch_count < 1
        throw(ArgumentError("min_batch_count must be ≥ 1, got $min_batch_count"))
    end

    if max_batch_size < 1
        throw(ArgumentError("max_batch_size must be ≥ 1, got $max_batch_size"))
    end

    # Split collection into batches, then peek at the first few batches
    batches = partition(c, max_batch_size)
    head, tail = head_and_tail(batches, min_batch_count)

    # If there are not enough batches, use a smaller batch size
    if length(head) < min_batch_count
        batch_size = max(1, div(sum(length, head), min_batch_count))
        return partition(collect(flatten(head)), batch_size)
    end

    return flatten((head, tail))
end
