# Extract, preprocess and select attributes from a collection of tweet messages (to be filtered)
# run as:
# JULIA_PROJECT=. julia -p32 -L src/extract-messages-from-tweets.jl
# or
# JULIA_PROJECT=. srun --pty -N1 -xgeoint0 julia -p64 -L src/messages-from-raw-tweets.jl


using CSV, DataFrames, JLD2, StatsBase, TextSearch
include("io.jl")

function extract_messages(filename; lang="es")
    @info filename
    L = []

    valid_cc = Set(VALID_CC)
    eachtweet(filename) do tweet
        if language(tweet) == lang
            cc = countrycode(tweet)
            (isnothing(cc) || !(cc in valid_cc)) && return

            lat, long = try
                mean(tweet["place"]["bounding_box"]["coordinates"][1])
            catch e
                showerror(stderr, e)
                return
            end

            t = preprocess(text(tweet))
            occursin("_url", t) && return
            (startswith(t, "RT") || startswith(t, "I'm at") || count(c->c == ' ', t) <= 6) && return

            id = tweet["id"]
            user_id = tweet["user"]["id"]
            screen_name = tweet["user"]["screen_name"]
            timestamp = parse(Int, tweet["timestamp_ms"])
            push!(L, (id, user_id, screen_name, cc, long, lat, timestamp, t))
        end
    end

    L
end

function process_directory(files, outdir, outname)
    println(stderr, "processing $(length(files)) files => $(outdir)")

    L = DataFrame(
        id=Int64[],
        user_id=Int64[],
        screen_name=String[],
        country_code=String[],
        long=Float64[],
        lat=Float64[],
        timestamp_ms=Int64[],
        text=String[],
    )
    for filename in files
        for r in extract_messages(filename)
            push!(L, r)
        end
    end

    for g in groupby(L, :country_code)
        cc = first(g.country_code)
        output = joinpath(outdir, cc, outname)
        open(output, "w") do f
            gz = GzipCompressorStream(f)
            CSV.write(gz, g, delim='\t')
            close(gz)
        end
    end
end

function process_directory_list(dirlist, outdir, pat="GEO*.log.gz")
    println(stderr, "processing $(length(dirlist)) directories")
    
    for d in dirlist
        for cc in VALID_CC
            mkpath(joinpath(outdir, cc))
        end
    end

    @sync @distributed for d in dirlist
        outname = replace(d, "/" => "_") * ".tsv.gz"
        process_directory(glob(joinpath(d, pat)), outdir, outname)
    end
end

function main(lang)
    dirlist = glob("DatasetSpanish/16/*/*")
    append!(dirlist, glob("DatasetSpanish/17/*/*"))
    append!(dirlist, glob("DatasetSpanish/18/*/*"))
    append!(dirlist, glob("DatasetSpanish/19/*/*"))
    outdir = "data/$lang/messages"
    process_directory_list(dirlist, outdir)

    #=process_directory_list(glob("DatasetSpanish/17/*/*"), lang)
    process_directory_list(glob("DatasetSpanish/18/*/*"), lang)
    process_directory_list(glob("DatasetSpanish/19/*/*"), lang)=#
end