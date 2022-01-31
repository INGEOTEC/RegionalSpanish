using JLD2, Glob, CSV, DataFrames, CodecZlib

function compute_voc_table(lang)
    VOC = DataFrame(token=String[], country_code=String[], ndocs=Int[], idf=Float64[])
    STATS = DataFrame(country_code=String[], n=Int[], rawvoc=Int[], voc=Int[])
    ALL = Dict{String,Int}()
    N = 0

    for filename in glob("data/$lang/messages-by-region/voc-*.jld2")
        @info filename
        n, m, voc = load(filename, "n", "m", "voc")
        cc = basename(filename)[5:6]
        N += n
        push!(STATS, (cc, n, m, length(voc)))
        for (token, ndocs) in voc
            prob = (ndocs + 1) / n
            push!(VOC, (token, cc, ndocs, log(1 / prob)))
            ALL[token] = get(ALL, token, 0) + ndocs
        end
    end

    for (token, ndocs) in ALL
        prob = (ndocs + 1) / N
        push!(VOC, (token, "ALL", ndocs, log(1 / prob)))
    end
    M = length(ALL)
    push!(STATS, ("ALL", N, M, M))
    sort!(VOC, :token)

    @info "saving voc files"
    open("data/$lang/voc.tsv.gz", "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, VOC, delim='\t')
        close(gz)
    end

    open("data/$lang/voc-stats.tsv.gz", "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, STATS, delim='\t')
        close(gz)
    end
end

