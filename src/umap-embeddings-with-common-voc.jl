# adapted from https://github.com/sadit/SimilaritySearchDemos/blob/main/Glove/create-index-and-umap.jl
# relased under the MIT licensing
#
# Both SimilaritySearch.jl and UMAP.jl take advantage of multithreading parallelism
# run as:
# julia -t64 -L src/umap-embedding-with-common-voc.jl

using SimilaritySearch, UMAP, LinearAlgebra, JLD2, Embeddings, Glob, CSV, DataFrames

function create_index(valid_tokens, modelname)
    E = Embeddings.load_embeddings(FastText_Text, modelname)
    mask = [(token in valid_tokens) for token in E.vocab]
    X = Matrix(E.embeddings[:, mask])
    vocab = E.vocab[mask]
    for c in eachcol(X)
        normalize!(c)
    end
    @show modelname => (size(vocab), size(X))
    dist = NormalizedCosineDistance()
    index = SearchGraph(; dist, db=MatrixDatabase(X))
    index.neighborhood.reduce = SatNeighborhood()
    push!(index.callbacks, OptimizeParameters(; kind=ParetoRecall()))
    index!(index; parallel_block=512)
    optimize!(index, OptimizeParameters(; kind=MinRecall(), minrecall=0.9))
    index, vocab
end

function main(lang)
    edir = "data/$lang/embeddings"
    cclist = [first(split(basename(modelname), '.')) for modelname in glob("$edir/*.vec")]
    common = CSV.read("data/SpanishLang/common-tokens-per-region.tsv.gz", DataFrame, delim='\t')
    subset!(common, :n_regions => n -> n .> 10)
    valid_tokens = Set(common.token)

    for cc in reverse(cclist)
        k = 33
        umapfile = "$edir/umap+index-common-tokens-$cc.k=$k.jld2"
        embfile = "$edir/umap-embeddings-common-tokens-$cc.jld2"
        @info cc
        isfile(embfile) && continue
        U2 = if isfile(umapfile)
            load(umapfile, "U2")
        else
            index, vocab = create_index(valid_tokens, "$edir/$cc.vec")
            U2 = UMAP_(index, 2; n_neighbors=k, init=:random)
            vocab = Dict(token => i for (i, token) in enumerate(vocab))
            jldsave(umapfile; U2, k, vocab)
            U2
        end

        U2 = UMAP_(index, 2; n_neighbors=k, init=:random)  # spectral layout is too slow for the input-data's size
        U3 = UMAP_(U2, 3; init=:random)  # reuses input data
        jldsave(embfile, e2=U2.embedding, e3=U3.embedding)
    end
end
