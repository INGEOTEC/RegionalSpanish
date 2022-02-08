using JLD2, JSON, CSV, DataFrames, CodecZlib

edir = "data/SpanishLang/embeddings"
k = 33
common = CSV.read("data/SpanishLang/common-tokens-per-region.tsv.gz", DataFrame, delim='\t')
subset!(common, :n_regions => n -> n .> 10)
cclist = split(first(subset(common, :n_regions => n -> n .== 26).country_codes), ',')
@info sort!(cclist)

function encode_model(vocab, umapmodel)
    #  ivocab = Dict(token => i for (i, token) in enumerate(vocab))
    obj = Dict{String,Vector}()
    
    for (i, (idcol, distcol)) in enumerate(zip(eachcol(umapmodel.knns), eachcol(umapmodel.dists))) 
        obj[vocab[i]] = [vocab[j] for j in idcol]
    end

    obj
end

#db = Dict()
for modelname in sort(glob(joinpath(edir, "umap+index-common-tokens-*.k=$k.jld2")))
    model_, vocab_ = load(modelname, "U2", "vocab")

    cc = replace(modelname, r".+tokens-" => "", r".k=.+" => "")
    v = encode_model(vocab_, model_)
    #v = db[cc]
    open("data/SpanishLang/common-voc-semantic-knn-$cc.json.gz", "w") do f
        gz = GzipCompressorStream(f)
        println(gz, JSON.json(v))
        close(gz)
    end
end
