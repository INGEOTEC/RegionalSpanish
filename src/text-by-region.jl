using CSV, Glob, DataFrames, CodecZlib

function save_text(lang="SpanishLang")
    L = glob("data/$lang/messages-by-region/*/*.tsv.gz")    
    outdir = "data/$lang/embeddings/"
    L = glob("data/$lang/messages-by-region/*/")

    @sync @distributed for i in eachindex(L)
        indir = rstrip(L[i], '/')
        cc = basename(indir)
        outname = "data/$lang/embeddings/$cc.txt"
        # isfile(outname) && continue

        open(outname * ".tmp", "w") do f
            for filename in glob(joinpath(indir, "*.tsv.gz"))
                @info filename => outname
                D = CSV.read(filename, DataFrame, delim='\t')

                for t in D.text
                    println(f, t)
                end
            end
        end

        mv(outname * ".tmp", outname; force=true)
    end
end