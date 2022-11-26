# adapted from https://github.com/sadit/SimilaritySearchDemos/blob/main/Glove/create-index-and-umap.jl
# relased under the MIT licensing
#
# to take advantage of multithreading parallelism run as:
# julia -t64 -L src/umap-embedding-with-common-voc.jl

using SimilaritySearch, SimSearchManifoldLearning, LinearAlgebra, JLD2, FileIO, Embeddings, Glob, CSV, DataFrames

function create_index(valid_tokens, modelname)
    
    E = Embeddings.load_embeddings(FastText_Text, modelname)
    mask = [(token in valid_tokens) for token in E.vocab]
    X = Matrix(E.embeddings[:, mask])
    vocab = E.vocab[mask]
    
    #=vocab = rand(1:100, 10000)
    X = rand(Float32, 8, 10000)=#
    for c in eachcol(X)
        normalize!(c)
    end
    @show modelname => (size(vocab), size(X))
    dist = NormalizedCosineDistance()
    G = SearchGraph(; dist, db=MatrixDatabase(X), verbose=false)
    opt = MinRecall(0.9)
    callbacks = SearchGraphCallbacks(opt)
    index!(G; callbacks)
    optimize!(G, opt)
    G, vocab
end

function main(lang)
    edir = "data/$lang/embeddings"
    cclist = [first(rsplit(basename(modelname), '.'; limit=2)) for modelname in glob("$edir/*.vec")]
    common = CSV.read("data/$lang/voc/ALL.tsv.gz", DataFrame, delim='\t')
    subset!(common, :n_regions => n -> n .>= 5)
    valid_tokens = Set(common.token)
    k = 33
    
    for cc in cclist
        cc == "GQ" && continue
        knnsfile = joinpath(edir, "knns-common-tokens.cc=$cc.k=$k.h5")
        embfile = joinpath(edir, "umap-embeddings-common-tokens.cc=$cc.k=$k.h5")
        isfile(embfile) && continue
        @info k, cc, knnsfile, embfile
        G, vocab = create_index(valid_tokens, "$edir/$cc.vec")
        knns, dists = allknn(G, k)
        jldsave(knnsfile; knns, dists, vocab, k)
        try
            U3 = fit(UMAP, G; maxoutdim=3, k, layout=RandomLayout(), tol=1e-4, n_epochs=100)
            U2 = fit(U3, 2)
            jldsave(embfile; e2=predict(U2), e3=predict(U3), vocab, k)
        catch e
            if e isa ArgumentError
                @info "ERROR computing UMAP for $cc -- small vocabularies wrt common voc are expected to fail"
                @show length(valid_tokens), length(vocab)
            else
                rethrow()
            end
        end
    end
end
