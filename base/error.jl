# This file is a part of Julia. License is MIT: http://julialang.org/license

# pseudo-definitions to show how everything behaves
#
# throw(label, val) = # throw a value to a dynamically enclosing block
#
# function rethrow(val)
#     global current_exception = val
#     throw(current_handler(), current_exception)
# end
#
# rethrow() = rethrow(current_exception)
#
# function throw(val)
#     global catch_backtrace = backtrace()
#     rethrow(val)
# end

## native julia error handling ##

error(s::AbstractString) = throw(Main.Base.ErrorException(s))
error(s...) = throw(Main.Base.ErrorException(Main.Base.string(s...)))

rethrow() = ccall(:jl_rethrow, Bottom, ())
rethrow(e) = ccall(:jl_rethrow_other, Bottom, (Any,), e)
backtrace() = ccall(:jl_backtrace_from_here, Array{Ptr{Void},1}, (Int32,), false)
catch_backtrace() = ccall(:jl_get_backtrace, Array{Ptr{Void},1}, ())

## keyword arg lowering generates calls to this ##
kwerr(kw) = error("unrecognized keyword argument \"", kw, "\"")

## system error handling ##

systemerror(p, b::Bool; extrainfo=nothing) = b ? throw(Main.Base.SystemError(string(p), Libc.errno(), extrainfo)) : nothing

## assertion functions and macros ##

assert(x) = x ? nothing : throw(Main.Base.AssertionError())
macro assert(ex, msgs...)
    msg = isempty(msgs) ? ex : msgs[1]
    if !isempty(msgs) && (isa(msg, Expr) || isa(msg, Symbol))
        # message is an expression needing evaluating
        msg = :(Main.Base.string($(esc(msg))))
    elseif isdefined(Main, :Base) && isdefined(Main.Base, :string)
        msg = Main.Base.string(msg)
    else
        # string() might not be defined during bootstrap
        msg = :(Main.Base.string($(Expr(:quote,msg))))
    end
    :($(esc(ex)) ? $(nothing) : throw(Main.Base.AssertionError($msg)))
end


"""
    retry(f, [on_error]; n=3; max_delay=10) -> Function

Returns a lambda that retries function `f` up to `n` times in the
event of an exception. If `on_error` is a `Type` then retry only
for exceptions of that type. If `on_error` is a function
`cond(::Exception) -> Bool` then retry only if it is true.

**Examples**
```julia
retry(http_get, e -> e.status == "503")(url)
retry(read, UVError)(io)
```
"""
DEFAULT_RETRY_N = 1
DEFAULT_RETRY_ON_ERROR = e->true
DEFAULT_RETRY_MAX_DELAY = 0
function retry(f::Function, on_error::Function=DEFAULT_RETRY_ON_ERROR; n=DEFAULT_RETRY_N, max_delay=DEFAULT_RETRY_MAX_DELAY)
    (args...) -> begin
        delay = 0.05
        for i = 1:n+1
            try
                return f(args...)
            catch e
                if i > n || try on_error(e) end != true
                    rethrow(e)
                end
            end
            sleep(delay * (0.8 + (rand() * 0.4)))
            delay = min(max_delay, delay * 5)
        end
    end
end

retry(f::Function, t::Type; kw...) = retry(f, e->isa(e, t); kw...)
