using JLD2, Glob

println(stderr, "NOT checked to work, I copied and pasted from repl")
exit(0)


VOC = let
    VOC = DataFrame(token=String[], country_code=String[], ndocs=Int[], idf=Float64[])
    for k in K
           Ndocs = 0
           N = 0
           for (cc, n, m, voc) in P
               ndocs = get(voc, k, 0)
               Ndocs += ndocs
               N += n
               prob = (ndocs + 1) / n
               push!(VOC, (k, cc, ndocs, log(1 / prob)))
           end
           prob = (Ndocs + 1) / N
           push!(VOC, (k, "ALL", Ndocs, log(1 / prob)))
    end
    VOC
end

P = let 
    P = []
    
    for filename in glob("data/messages-by-region/voc-*.jld2")
        n, m, voc = load(filename, "n", "m", "voc")
        cc = basename(filename)[5:6]
        push!(P, (cc, n, m, voc))
    end
    P
end

open("data/voc.tsv.gz", "w") do f
    gz = GzipCompressorStream(f)
    CSV.write(gz, VOC, delim='\t')
    close(gz)
end

ndocs = [(a, b, c, length(d)) for (a, b, c, d) in P]
M = unique(VOC.token) |> length
push!(ndocs, ("ALL", all, M, M))
D = DataFrame(country_code=String[], n=Int[], rawvoc=Int[], voc=Int[])
for d in ndocs
    push!(D, d)
end

open("data/voc-stats.tsv.gz", "w") do f
    gz = GzipCompressorStream(f)
    CSV.write(gz, D, delim='\t')
    close(gz)
end