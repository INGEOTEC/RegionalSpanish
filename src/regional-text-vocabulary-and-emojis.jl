# extracts vocabularies and emojis per region
# run as:
# JULIA_PROJECT=. julia -p64 -Lsrc/vocabulary-and-emojis.jl
# and then main(lang)

using CSV, Glob, DataFrames, JLD2, CodecZlib, TextSearch, InvertedFiles

include("io.jl")

function vector_model(filename, mindocs)
    voc = Vocabulary(1000)
    n = 0
    bow = Dict{String,Int}()
    for line in eachline(filename)
        n += 1
        empty!(bow)
        for w in split(line)
            bow[w] = get(bow, w, 0) + 1
        end

        for (w, occs) in bow
            TextSearch.push_token!(voc, w, occs, 1)
        end
    end

    voc = filter_tokens(Vocabulary(token(voc), occs(voc), ndocs(voc), voc.token2id, n)) do t
        t.ndocs >= mindocs
    end
    
    VectorModel(IdfWeighting(), BinaryLocalWeighting(), voc)
end

function vocabulary_table(lang, mindocs=5)
    DATA = "data/$lang/"
    vocfile = joinpath(DATA, "voc.tsv.gz")
    vocstatsfile = joinpath(DATA, "voc-stats.tsv.gz")
    
    #=if isfile(vocfile) && isfile(vocstatsfile)
        @info "vocabulary files already exists"
        return
end=#
    
    VOC = DataFrame(token=String[], country_code=String[], ndocs=Int[], idf=Float64[])
    STATS = DataFrame(country_code=String[], n=Int[], rawvoc=Int[], voc=Int[])
    ALL = Dict{String,Int}()
    N = 0
    
    vocpath = joinpath(DATA, "voc")
    mkpath(vocpath)
    M = Vocabulary[]
    lock_ = Threads.SpinLock()
    
    # Threads.@threads
    Dcc = Dict{String,Vector{String}}()
    for i in eachindex(VALID_CC)
        cc = VALID_CC[i]
        filename = joinpath(DATA, "messages", cc * ".txt")
        @info filename
        name_ = replace(basename(filename), ".txt" => "")
        name_ = joinpath(vocpath, name_)
        name = name_ * ".jld2"
        
        model = if isfile(name)
            @info "loading vector model for $filename"
            load(name, "model")
        else
            @info "creating vector model for $filename"
            model = vector_model(filename, mindocs)
            jldsave(name; model)            
            model
        end
        
        VOC = DataFrame(token=token(model), occs=occs(model), ndocs=ndocs(model), weight=weight(model))
        open(name_ * ".tsv.gz", "w") do f
            gz = GzipCompressorStream(f)
            CSV.write(gz, VOC, delim='\t')
            close(gz)
        end
        
        lock(lock_)
        try
            for t in token(model)
                lst = get(Dcc, t, nothing)
                if lst === nothing
                    Dcc[t] = [cc]
                else
                    push!(lst, cc)
                end
            end
            
            if length(M) == 0
                push!(M, model.voc)
            else
                M[1] = merge_voc(only(M), model.voc)
            end
        finally
            unlock(lock_)
        end
    end
    
    model = VectorModel(IdfWeighting(), BinaryLocalWeighting(), only(M))
    name_ = joinpath(vocpath, "ALL")
    jldsave(name_ * ".jld2"; model)
    
    for v in values(Dcc)
        sort!(v)
    end
    
    VOC = DataFrame(
        token=token(model),
        occs=occs(model),
        ndocs=ndocs(model),
        weight=weight(model),
        n_regions=[length(Dcc[t]) for t in token(model)],
        country_codes=[join(Dcc[t], ':') for t in token(model)]
    )
    open(name_ * ".tsv.gz", "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, VOC, delim='\t')
        close(gz)
    end
    
end

#=function count_emojis(filename)
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
end=#
    
function emoji_table(lang)
    outname = "data/$lang/emojis.tsv.gz"
    # isfile(outname) && return

    D = Dict()
    for cc in VALID_CC
        name = "data/$lang/voc/$cc.tsv.gz"
        @info "loading $name"
        V = CSV.read(name, DataFrame, delim='\t')
        
        for row in eachrow(V)
            for e in emojis(row.token)
                D[(e, cc)] = get(D, (e, cc), 0) + row.ndocs
            end
        end
    end
    
    E = DataFrame(emoji=Char[], country_code=[], ndocs=Int[])

    for (k, freq) in D
        e, cc = k
        push!(E, (e, cc, freq))
    end

    sort!(E, :emoji)

    @info "saving $outname"
    open(outname, "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, E, delim='\t')
        close(gz)
    end
end

function save_list_(file, L)
    for lst in L
        for t in lst
            println(file, t)
        end
    end
end

function save_list(outname::String, L::AbstractVector)
    @info "saving $outname"
    open(outname * ".tmp", "w") do f
        if endswith(outname, ".gz")
            gz = GzipCompressorStream(f)
            save_list_(gz, L)
            close(gz)
        else
            save_list_(f, L)
        end
    end

    mv(outname * ".tmp", outname; force=true)
end

get_text_config() = TextConfig(lc=true, del_diac=true, del_dup=true, del_punc=false, group_num=true)

function extract_text_and_id_(filename::String)
    config = get_text_config()
    @info "loading $filename"
    D = CSV.read(filename, DataFrame, delim='\t')
    tokenize_corpus(tokens -> join(tokens, ' '), config, D.text), D.id
end

function extract_text_and_id(ccpath)
    outname = rstrip(ccpath, '/')
    
    isfile(outname * ".txt") && return
    I = []
    L = []
    for filename in glob(joinpath(ccpath, "*.tsv.gz"))
        L_, I_ = extract_text_and_id_(filename)
        push!(L, L_)
        push!(I, I_)
    end
    
    save_list(outname * ".txt", L)
    save_list(outname * "-id-list.txt.gz", I)
end

function main(lang)
    @assert lang == :es
    lang = "SpanishLang"
    
    cclist = sort!(glob("data/$lang/messages/*/"), rev=true)
    for ccpath in cclist
        extract_text_and_id(ccpath)
    end

    vocabulary_table(lang)
    emoji_table(lang)
end
