using CSV, Glob, DataFrames, CodecZlib


function main(lang)
    L = glob("data/$lang/messages-by-region/*/*.tsv.gz")
    
    outdir = "data/$lang/id-by-region"
    for cc in glob("data/$lang/messages-by-region/*/")
        cc = basename(rstrip(cc, '/'))
        mkpath(joinpath(outdir, cc))
    end

    @sync @distributed for i in eachindex(L)
        filename = L[i]
        arr = splitpath(filename)
        outname = first(split(arr[end], '.'))
        outname = joinpath(outdir, arr[end-1], outname) * ".txt.gz"
        # isfile(outname) && continue
        @info filename => outname
        D = try
            CSV.read(filename, DataFrame, delim='\t')
        catch
            @info "error $filename => $outname, skipping file"
            continue
        end

        open(outname * ".tmp", "w") do f
            gz = GzipCompressorStream(f)
            for id in D.id
                println(gz, id)
            end

            close(gz)
        end

        mv(outname * ".tmp", outname; force=true)
    end
end