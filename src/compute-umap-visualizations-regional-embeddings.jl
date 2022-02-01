using Pkg
Pkg.activate("scripts")
#Pkg.add(["Plots", "CSV", "DataFrames", "SimilaritySearch", "TextSearch", "Formatting", "Latexify", "UMAP", "Glob"])
using Plots, JLD2, LinearAlgebra, Glob

files = glob("data/SpanishLang/embeddings/umap-embeddings-*.jld2")

for filename in files
    @info filename
    X, C = load(filename, "e2", "e3")

    for i in (1, 2, 3)
        min_, max_ = extrema(C[i, :])
        # @info i => (min_, max_)
        for j in 1:size(C, 2)
            C[i, j] = (C[i, j] - min_) / (max_ - min_)
        end
    end
    
    C = [RGB(c...) for c in eachcol(C)]

    title = last(split(replace(basename(filename), ".jld2" => ""), '-'))
    # scatter(X[1, :], X[2, :], c=C, label="", series_annotations=text.(cclist, :bottom), ms=6, ma=0.7, title=title)
    display(scatter(X[1, :], X[2, :], c=C, label="", ms=6, ma=0.7, title=title, fmt=:png))
end