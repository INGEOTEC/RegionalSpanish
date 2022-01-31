# extracts emojis from regional vocabularies
# run as:
# JULIA_PROJECT=. julia -p64 -Lsrc/extract-emojis-per-region.jl
# and then main()
using JLD2, DataFrames, CSV, Glob, TextSearch, CodecZlib
include("io.jl")

function count_emojis(filename)
    voc = load(filename, "voc")

    D = Dict{Char,Int}()
    for (token, freq) in voc
        # there are some cases when the tokenizer considers emojis as part of a word, e.g.,
        # when they start the word
        lst = emojis(token)
        if length(lst) > 0
            for e in lst
                D[e] = get(D, e, 0) + freq
            end
        end
    end
    
    replace(basename(filename), "voc-" => "", ".jld2" => "") => D
end
    
function main(lang)
    R = pmap(count_emojis, glob("data/$lang/messages-by-region/voc-*.jld2"))
    E = DataFrame(emoji=Char[], country_code=[], freq=Int[])
    for (cc, D) in R
        println(cc => length(D))
        for (emoji, freq) in D
            push!(E, (emoji, cc, freq))
        end
    end

    sort!(E, :emoji)

    outname = "data/$lang/emojis.tsv.gz"
    open(outname, "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, E, delim='\t')
        close(gz)
    end
end