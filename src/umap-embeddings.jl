# adapted from https://github.com/sadit/SimilaritySearchDemos/blob/main/Glove/create-index-and-umap.jl
# relased under the MIT licensing
#
# Both SimilaritySearch.jl and UMAP.jl take advantage of multithreading parallelism
# run as:
# julia -t64 -L src/compute-all-knn-embedding.jl

using SimilaritySearch, UMAP, LinearAlgebra, JLD2, PyCall, Glob

ft = pyimport("fasttext")

function create_or_load_index(indexfile, modelname)
    if isfile(indexfile)
		load(indexfile, "index", "vocab")
	else
        X, vocab = let
            model = ft.load_model(modelname)
            X = Matrix(transpose(model.get_output_matrix()))
            vocab = copy(model.get_words())
            for c in eachcol(X)
                normalize!(c)
            end
            X, vocab
        end

        dist = NormalizedCosineDistance()
		index = SearchGraph(; dist, db=MatrixDatabase(X))
		index.neighborhood.reduce = SatNeighborhood()
		push!(index.callbacks, OptimizeParameters(; kind=ParetoRecall()))
		index!(index; parallel_block=1024)
		optimize!(index, OptimizeParameters(; kind=MinRecall(), minrecall=0.9))
		jldsave(indexfile, index=index, vocab=vocab)
		index, vocab
	end
end

function main(lang)
    edir = "data/$lang/embeddings"
    cclist = [first(split(basename(modelname), '.')) for modelname in glob("$edir/*.bin")]
    
    for cc in cclist
        embfile = "$edir/umap-embeddings-$cc.jld2"

        if !isfile(embfile) 
            index, vocab = create_or_load_index("$edir/index-$cc.jld2", "$edir/$cc.bin")
            U2 = UMAP_(index, 2; init=:random)  # spectral layout is too slow for the input-data's size
            U3 = UMAP_(U2, 3; init=:random)  # reuses input data
            jldsave(embfile, e2=U2.embedding, e3=U3.embedding, vocab=vocab)
        end
    end
end
