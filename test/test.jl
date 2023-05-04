using Random

@testset "Random QCSR data" begin
    N = 2^10
    L = 300

    for T in [Bool, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64, ComplexF32, ComplexF64]
        @testset "Type $(string(T))" begin
            data = QcsrChunk{T}[]

            for i in Base.OneTo(N)
                len = rand(0:L)
                push!(data, BitVector(rand(Bool, len)) => rand(T))
            end

            @test length(data) == N

            tmpfile, tmpio = mktemp()
            close(tmpio)

            QCSR.save(tmpfile, data)

            data2 = QCSR.load(tmpfile)

            @test data == data2
        end
    end
end
