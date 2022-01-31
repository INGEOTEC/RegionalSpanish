using CSV, Glob, DataFrames, CodecZlib

function load_data(files)
    CSV.read(files, DataFrame, delim='\t')
end

function separate_messages(y, m, files, cclist, outdir)
    @info "Loading files $files"
    D = load_data(files)
    dropmissing!(D)
    
    #Threads.@threads 
    for i in eachindex(cclist)
        cc = cclist[i]
        outname = joinpath(outdir, cc, "messages-$(y)-$(m)-$(cc).tsv.gz")
        #isfile(outname) && continue
        E = D[D.country_code .== cc, :]
        @info (y, m, cc, size(E)) => outname
        open(outname * ".tmp", "w") do f
            gz = GzipCompressorStream(f)
            CSV.write(gz, E, delim='\t')
            close(gz)
        end

        mv(outname * ".tmp", outname; force=true)
    end
end

function main(lang)
    cclist = ["AR", "BO", "BR", "CA", "CL", "CO", "CR", "CU", "DO", "EC", "ES", "FR", "GB", "GQ", "GT", "HN", "MX", "NI", "PA", "PE", "PR", "PY", "SV", "US", "UY", "VE"]
    y = 16
    outdir = "data/$lang/messages-by-region"
    ylist = [16, 17, 18, 19]
    mlist = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    args = []

    if length(args) == 0
        for y in ylist, m in mlist
            push!(args, (y, m))
        end
    end

    for cc in cclist
        mkpath(joinpath(outdir, cc))
    end

    #return length(args), args
    @sync @distributed for i in eachindex(args)
        y, m = args[i]
        files = glob("data/$lang/messages-by-year/$y/DatasetSpanish_$(y)_$(m)_*.tsv.gz")
        sort!(files)
        separate_messages(y, m, files, cclist, outdir)
    end
end