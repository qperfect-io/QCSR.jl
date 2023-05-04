#
# Copyright © 2023 QPerfect.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module QCSR

export QcsrChunk
export QcsrFile

_write_littleendian(f::IO, v, T::Type) = write(f, htol(T(v)))
_read_littleendian(f::IO, T::Type) = T(ltoh(read(f, T)))
_read_littleendian(f::IO, n::Integer, T::Type) = T.(ltoh.([read(f, T) for _ in 1:n]))

# Define the QCSR file type
const QCSR_MAGIC = UInt8[0x51, 0x43, 0x53, 0x52, 0x00, 0x00, 0x00, 0x00]
const QCSR_EXTENSION = ".qcsr"
const QCSR_VERSION = 1

@enum QcsrDataType::UInt8 begin
    QCSR_DTYPE_BOOL = 1
    QCSR_DTYPE_CHAR = 2
    QCSR_DTYPE_UINT8 = 3
    QCSR_DTYPE_UINT16 = 4
    QCSR_DTYPE_UINT32 = 5
    QCSR_DTYPE_UINT64 = 6
    QCSR_DTYPE_INT8 = 7
    QCSR_DTYPE_INT16 = 8
    QCSR_DTYPE_INT32 = 9
    QCSR_DTYPE_INT64 = 10
    QCSR_DTYPE_FLOAT32 = 11
    QCSR_DTYPE_FLOAT64 = 12
    QCSR_DTYPE_COMPLEX64 = 13
    QCSR_DTYPE_COMPLEX128 = 14
end

"""
    convert_type(t::QCSRDataType)

Convert a QCSR type into the corresponding Julia type
"""
function convert_type(t::QcsrDataType)::Type
    t == QCSR_DTYPE_BOOL ? Bool :
    t == QCSR_DTYPE_CHAR ? Char :
    t == QCSR_DTYPE_UINT8 ? UInt8 :
    t == QCSR_DTYPE_UINT16 ? UInt16 :
    t == QCSR_DTYPE_UINT32 ? UInt32 :
    t == QCSR_DTYPE_UINT64 ? UInt64 :
    t == QCSR_DTYPE_INT8 ? Int8 :
    t == QCSR_DTYPE_INT16 ? Int16 :
    t == QCSR_DTYPE_INT32 ? Int32 :
    t == QCSR_DTYPE_INT64 ? Int64 :
    t == QCSR_DTYPE_FLOAT32 ? Float32 :
    t == QCSR_DTYPE_FLOAT64 ? Float64 :
    t == QCSR_DTYPE_COMPLEX64 ? ComplexF32 :
    t == QCSR_DTYPE_COMPLEX128 ? ComplexF64 :
    error("Not matched QCSR data type to Julia type")
end

"""
    convert_type(julia_type)

Converts a julia type into the corresponding QCSR type identifier.
"""
function convert_type(t::Type)::QcsrDataType
    t == Bool ? QCSR_DTYPE_BOOL :
    t == Char ? QCSR_DTYPE_CHAR :
    t == UInt8 ? QCSR_DTYPE_UINT8 :
    t == UInt16 ? QCSR_DTYPE_UINT16 :
    t == UInt32 ? QCSR_DTYPE_UINT32 :
    t == UInt64 ? QCSR_DTYPE_UINT64 :
    t == Int8 ? QCSR_DTYPE_INT8 :
    t == Int16 ? QCSR_DTYPE_INT16 :
    t == Int32 ? QCSR_DTYPE_INT32 :
    t == Int64 ? QCSR_DTYPE_INT64 :
    t == Float32 ? QCSR_DTYPE_FLOAT32 :
    t == Float64 ? QCSR_DTYPE_FLOAT64 :
    t == ComplexF32 ? QCSR_DTYPE_COMPLEX64 :
    t == ComplexF64 ? QCSR_DTYPE_COMPLEX128 :
    throw(
        ArgumentError(
            "Bad conversion. The Julia type `$(t)` does not correspond to any QCSR data type.",
        ),
    )
end

struct QcsrFile
    io::IO
    ownstream::Bool
end

function writeheader(io::IO)
    tot = 0
    for b in QCSR_MAGIC
        tot = _write_littleendian(io, b, UInt8)
    end

    tot += _write_littleendian(io, QCSR_VERSION, UInt32)
    tot += _write_littleendian(io, 0x00, UInt32)

    for _ in 1:16
        tot += _write_littleendian(io, 0x00, UInt8)
    end

    return tot
end

function readheader(io::IO)
    magic = _read_littleendian(io, 8, UInt8)

    version = _read_littleendian(io, UInt32)
    skip(io, 4)

    # reserved bytes
    skip(io, 16)

    return magic, version
end

function readheader(f::QcsrFile)
    magic, version = readheader(f.io)
    return magic, version
end

function writeheader(f::QcsrFile)
    writeheader(f.io)
end

function checkheader(::Type{T}, magic, version, dtype) where {T}
    if magic != QCSR_MAGIC
        error("Not a valid QCSR File")
    end

    if dtype != convert_type(T)
        error("Incompatible data type.")
    end

    if version > QCSR_VERSION
        error("Incompatible version of QCSR.")
    end
end

function skipheader(f::QcsrFile)
    readheader(f)
    f
end

function QcsrFile(::Type{T}, f::String, mode::String) where {T}
    QcsrFile{T}(open(f, mode), true)
end

# == Base stream methods == #

function Base.read(::QcsrFile, ::Type{<:Any})
    throw(ArgumentError("Can only read QcsrChunk from QCSR files"))
end

function Base.write(::QcsrFile, ::Any)
    throw(ArgumentError("Can only read QcsrChunk from QCSR files"))
end

function Base.close(reader::QcsrFile)
    reader.ownstream && close(reader.io)
end

Base.eof(f::QcsrFile) = eof(f.io)

Base.seekstart(f::QcsrFile) = seekstart(f.io)
Base.seekend(f::QcsrFile) = seekend(f.io)

Base.iswritable(f::QcsrFile) = iswritable(f.io)
Base.isreadable(f::QcsrFile) = isreadable(f.io)
Base.isreadonly(f::QcsrFile) = isreadonly(f.io)
Base.flush(f::QcsrFile) = flush(f.io)

# == FileIO like interface == #

const QcsrChunk{T} = Pair{BitVector,T} where {T}

function Base.read(f::QcsrFile, ::Type{QcsrChunk})
    len = _read_littleendian(f.io, UInt64)

    dtype = QcsrDataType(_read_littleendian(f.io, UInt8))
    skip(f.io, 7)

    bs = BitVector(_read_littleendian(f.io, len, Bool))

    d = _read_littleendian(f.io, convert_type(dtype))

    return bs => d
end

function Base.write(f::QcsrFile, pair::QcsrChunk{T}) where {T}
    bs = first(pair)
    tot = _write_littleendian(f.io, length(bs), UInt64)

    dtype = convert_type(T)
    tot += _write_littleendian(f.io, dtype, UInt8)

    for _ in 1:7
        tot += _write_littleendian(f.io, 0x00, UInt8)
    end

    for b in bs
        tot += _write_littleendian(f.io, b, Bool)
    end

    d = last(pair)

    tot += _write_littleendian(f.io, d, T)
    return tot
end

function Base.read(reader::QcsrFile, n::Integer)
    map(1:n) do
        read(reader, QcsrChunk)
    end
end

function Base.read(reader::QcsrFile)
    res = QcsrChunk[]
    while !eof(reader)
        chunk = read(reader, QcsrChunk)
        push!(res, chunk)
    end
    res
end

function Base.write(f::QcsrFile, chunks::Vector{<:QcsrChunk})
    mapreduce(+, chunks) do chunk
        write(f, chunk)
    end
end

function loadstreaming(f::String)
    reader = QcsrFile(open(f, "r"), true)
    skipheader(reader)
    reader
end

loadstreaming(s::IO) = QcsrFile(s, false)

function loadstreaming(f::Function, q)
    reader = loadstreaming(q)
    res = f(reader)
    close(reader)
    res
end

function savestreaming(f::String)
    writer = QcsrFile(open(f, "w"), true)
    writeheader(writer)
    writer
end

savestreaming(io::IO) = QcsrFile(io, false)

function savestreaming(f::Function, s)
    writer = savestreaming(s)
    res = f(writer)
    close(writer)
    res
end

function save(q::Union{String,IO}, data, args...; kwargs...)
    savestreaming(q, args...; kwargs...) do file
        write(file, data)
    end
end

function load(q::Union{String,IO}, args...; kwargs...)
    loadstreaming(q, args...; kwargs...) do stream
        read(stream, args...)
    end
end

end # module QCSR
