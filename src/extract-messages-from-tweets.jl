# Extract, preprocess and select attributes from a collection of tweet messages (to be filtered)
# run as:
# JULIA_PROJECT=. julia -p32 -L src/extract-messages-from-tweets.jl


using CSV, DataFrames, JLD2, StatsBase, TextSearch
include("io.jl")


function extract_messages(filename; lang="es")
    config = TextConfig(lc=true, del_diac=true, del_dup=true, del_punc=false, group_num=true)
    tok = Tokenizer(config)
    mintokens = 7

    @info filename
    L = []

    eachtweet(filename) do tweet
        if language(tweet) == lang
            t = preprocess(text(tweet))

            startswith(t, "rt") && return
            startswith(t, "i'm at") && return
            tokens = tokenize(tok, t)
            length(tokens) < mintokens && return
            t = join(decode.(tok, tokens), ' ')
          
            cc = countrycode(tweet)
            isnothing(cc) && return

            lat, long = try
                mean(tweet["place"]["bounding_box"]["coordinates"][1])
            catch e
                showerror(stderr, e)
                return
            end

            id = tweet["id"]
            user_id = tweet["user"]["id"]
            screen_name = tweet["user"]["screen_name"]
            timestamp = parse(Int, tweet["timestamp_ms"])
            push!(L, (id, user_id, screen_name, cc, long, lat, timestamp, t))
        end
    end

    L
end

function process_directory(batch)
    if isfile(batch.outname)
        println(stderr, "$(batch.outname) already exist, jumping next batch")
        return 0
    end

    println(stderr, "processing $(length(batch.filenames)) files => $(batch.outname)")

    L = DataFrame(
        id=Int64[],
        user_id=Int64[],
        screen_name=String[],
        country_code=String[],
        long=Union{Float64,Missing}[],
        lat=Union{Float64,Missing}[],
        timestamp_ms=Int64[],
        text=String[],
    )
    for filename in batch.filenames
        for r in extract_messages(filename)
            push!(L, r)
        end
    end

    open(batch.outname, "w") do f
        gz = GzipCompressorStream(f)
        CSV.write(gz, L, delim='\t') # jldsave(batch.outname, messages=L)
        close(gz)
    end
end

#function main(; dirlist=glob("GEO/2021/*/*"), pat="*.gz", outpath="data")
function process_directory_list(dirlist, outpath, pat="GEO*.log.gz")
    println(stderr, "processing $(length(dirlist)) directories")
    batches = [
        (
            outname = joinpath(outpath, replace(d, "/" => "_") * ".tsv.gz"),
            filenames = glob(joinpath(d, pat))
        ) for d in dirlist
    ]
    mkpath(outpath)
    pmap(process_directory, batches)
end

function main()
    process_directory_list(glob("DatasetSpanish/16/*/*"), "data/$lang/messages-by-year/16")
    process_directory_list(glob("DatasetSpanish/17/*/*"), "data/$lang/messages-by-year/17")
    process_directory_list(glob("DatasetSpanish/18/*/*"), "data/$lang/messages-by-year/18")
    process_directory_list(glob("DatasetSpanish/19/*/*"), "data/$lang/messages-by-year/19")
end