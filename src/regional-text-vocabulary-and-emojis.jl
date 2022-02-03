# extracts vocabularies and emojis per region
# run as:
# JULIA_PROJECT=. julia -p64 -Lsrc/vocabulary-and-emojis.jl
# and then main(lang)

using CSV, Glob, DataFrames, JLD2, CodecZlib, TextSearch, InvertedFiles
using InvertedFiles: add!

include("io.jl")

function compute_vocabulary(filename, mindocs=5)
    voc = Dict{String,Int}()
    sizehint!(voc, 100_000)
    @info "computing vocabulary for $filename"

    n = 0
    doc = Set{String}()
    for t in eachline(filename)
        n += 1
        empty!(doc)

        for w in split(t)
            if !(w in doc)
                voc[w] = get(voc, w, 0) + 1
                push!(doc, w)
            end
        end
    end

    m = length(voc)

    for (k, freq) in voc
        if freq < mindocs
            delete!(voc, k)
        end
    end

    @info "computed vocabulary for $filename, n=$n, m=$m, final voc: $(length(voc))"
    voc, m, n, filename
end

function vocabulary_table(lang)
    vocfile = "data/$lang/voc.tsv.gz"
    vocstatsfile = "data/$lang/voc-stats.tsv.gz"
    if isfile(vocfile) && isfile(vocstatsfile)
        @info "vocabulary files already exists"
        return
    end
    VOC = DataFrame(token=String[], country_code=String[], ndocs=Int[], idf=Float64[])
    STATS = DataFrame(country_code=String[], n=Int[], rawvoc=Int[], voc=Int[])
    ALL = Dict{String,Int}()
    N = 0

    L = pmap(compute_vocabulary, glob("data/$lang/messages/*.txt"))

    for (voc, m, n, filename) in L
        cc = basename(filename)[1:2]
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
    open(vocfile, "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, VOC, delim='\t')
        close(gz)
    end

    open(vocstatsfile, "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, STATS, delim='\t')
        close(gz)
    end
end

function count_emojis(filename)
    voc = load(filename, "voc")

    D = Dict{Char,Int}()
    for (token, freq) in voc
        # there are some cases when the tokenizer considers emojis as part of a word, e.g.,
        # when they start the word
        lst = emojis(token)
        if length(lst) > 0
            for e in lst
                D[e] = get(D, e, 0) + row.ndocs
            end
        end
    end
    
    replace(basename(filename), "voc-" => "", ".jld2" => "") => D
end
    
function emoji_table(lang)
    outname = "data/$lang/emojis.tsv.gz"
    isfile(outname) && return

    V = CSV.read("data/$lang/voc.tsv.gz", DataFrame, delim='\t')
    E = DataFrame(emoji=Char[], country_code=[], freq=Int[])

    D = Dict()

    for row in eachrow(V)
        cc = row.country_code

        for e in emojis(row.token)
            D[(e, cc)] = get(D, (e, cc), 0) + row.ndocs
        end
    end

    for (k, freq) in D
        e, cc = k
        push!(E, (e, cc, freq))
    end

    sort!(E, :emoji)

    open(outname, "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, E, delim='\t')
        close(gz)
    end
end

function normalize_text_from_csv(filename)
    config = TextConfig(lc=true, del_diac=true, del_dup=true, del_punc=false, group_num=true)
    tok = Tokenizer(config)
    @info filename
    D = CSV.read(filename, DataFrame, delim='\t')
    T = Vector{String}(undef, length(D.text))

    for (i, t) in enumerate(D.text)
        tokens = tokenize(tok, t)
        T[i] = join(decode.(tok, tokens), ' ')
    end

    T
end

function extract_text(ccpath)
    outname = rstrip(ccpath, '/') * ".txt"
    isfile(outname) && return
    L = pmap(normalize_text_from_csv, glob(joinpath(ccpath, "*.tsv.gz")))

    open(outname * ".tmp", "w") do f
        for lst in L
            for t in lst
                println(f, t)
            end
        end
    end

    mv(outname * ".tmp", outname; force=true)
end

function extract_id(ccpath)
    outname = rstrip(ccpath, '/') * "-id-list.txt.gz"
    isfile(outname) && return
    
    open(outname * ".tmp", "w") do f
        gz = GzipCompressorStream(f)
        for filename in glob(joinpath(ccpath, "*.tsv.gz"))
            @info "extracting identifiers from $filename"
            D = CSV.read(filename, DataFrame, delim='\t')
            for id in D.id
                println(gz, id)
            end
        end

        close(gz)
    end

    mv(outname * ".tmp", outname; force=true)
end

function main(lang)
    
    for ccpath in glob("data/$lang/messages/*/")
        extract_text(ccpath)
    end

    @sync @distributed for ccpath in glob("data/$lang/messages/*/")
        extract_id(ccpath)
    end

    vocabulary_table(lang)
    emoji_table(lang)
end
