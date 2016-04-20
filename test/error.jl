# This file is a part of Julia. License is MIT: http://julialang.org/license

let
    function foo_error(c, n)
        c[1] += 1
        if c[1] <= n
            error("foo")
        end
        return 7
    end

    # Success on first attempt
    c = [0]
    @test retry(foo_error)(c,0) == 7
    @test c[1] == 1

    # Success on second attempt
    c = [0]
    @test retry(foo_error)(c,1) == 7
    @test c[1] == 2

    # Success on third attempt
    c = [0]
    @test retry(foo_error)(c,2) == 7
    @test c[1] == 3

    # 3 failed attempts, so exception is raised
    c = [0]
    ex = try retry(foo_error)(c,3); catch e; e; end
    @test ex.msg == "foo"
    @test c[1] == 3

    c = [0]
    ex = try (retry(foo_error, ErrorException))(c,3); catch e; e; end
    @test typeof(ex) == ErrorException
    @test ex.msg == "foo"
    @test c[1] == 3

    c = [0]
    ex = try (retry(foo_error, e->e.msg == "foo"))(c,3) catch e; e; end
    @test typeof(ex) == ErrorException
    @test ex.msg == "foo"
    @test c[1] == 3

    # No retry if condition does not match
    c = [0]
    ex = try (retry(foo_error, e->e.msg == "bar"))(c,3) catch e; e; end
    @test typeof(ex) == ErrorException
    @test ex.msg == "foo"
    @test c[1] == 1

    c = [0]
    ex = try (retry(foo_error, e->e.http_status_code == "503"))(c,3) catch e; e; end
    @test typeof(ex) == ErrorException
    @test ex.msg == "foo"
    @test c[1] == 1

    c = [0]
    ex = try (retry(foo_error, SystemError))(c,3) catch e; e; end
    @test typeof(ex) == ErrorException
    @test ex.msg == "foo"
    @test c[1] == 1
end
