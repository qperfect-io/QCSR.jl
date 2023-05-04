using QCSR
using Test

filelist = String["test.jl"]

@testset "QCSR.jl" begin
    @testset "$filename" for filename in filelist
        @debug "Running $filename"
        include(filename)
    end
end

nothing
