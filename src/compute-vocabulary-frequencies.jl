using CSV, Glob, DataFrames, JLD2, CodecZlib, TextSearch, InvertedFiles
using InvertedFiles: add!

function compute_vocabulary(filename)
    voc = Dict{String,Int}()
    sizehint!(voc, 100_000)
    @info filename

    D = CSV.read(filename, DataFrame, delim='\t')
    @info "readed $filename - $(length(D.text))"

    doc = Set{String}()
    for t in D.text
        empty!(doc)
        for w in split(t)
            if !(w in doc)
                voc[w] = get(voc, w, 0) + 1
                push!(doc, w)
            end
        end
    end

    voc, length(D.text)
end

function region_vocabulary(lang, cc, mindocs=5)
    P = pmap(compute_vocabulary, glob("data/$lang/messages-by-region/$cc/*.tsv.gz"))
    voc, n = P[1]
    for (voc_, n_) in P[2:end]
        n += n_
        if length(voc) < length(voc_)
            add!(voc_, voc)
            voc = voc_
        else
            add!(voc, voc_)
        end
    end

    m = length(voc)
    for (k, freq) in voc
        if freq < mindocs
            delete!(voc, k)
        end
    end

    jldsave("data/$lang/messages-by-region/voc-$cc.jld2", n=n, m=m, voc=voc)
    open("data/$lang/messages-by-region/voc-$cc.voc.gz", "w") do f
        gz = GzipCompressorStream(f)
        println(gz, "$n\t$m\t$(length(voc))")
        for (k, freq) in sort!(collect(voc), by=last, rev=true)
            println(gz, "$k\t$freq")
        end

        close(gz)
    end
end

function main(lang)
    for p in glob("data/$lang/messages-by-region/*/")
        p = rstrip(p, '/')
        cc = basename(p)
        region_vocabulary(lang, cc)
    end
end
