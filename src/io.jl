using JSON, CodecZlib, Glob

const VALID_CC = ["AR", "BO", "BR", "CA", "CL", "CO", "CR", "CU", "DO", "EC", "ES", "FR", "GB", "GQ", "GT", "HN", "MX", "NI", "PA", "PE", "PR", "PY", "SV", "US", "UY", "VE"]

function parse_tweet(line)
    if length(line) == 0
        return nothing
    end
    
    if line[1] != '{'
        line = split(line, '\t') |> last
    end
    
    try
        JSON.parse(line)
    catch
        nothing
    end
end

function eachtweet(callback, filename)
    open(filename) do f
        stream = endswith(filename, ".gz") ? GzipDecompressorStream(f) : f
        for line in eachline(stream)
            tweet = parse_tweet(line)
            tweet !== nothing && callback(tweet)
        end
    end
end

function text(tweet)
    et = get(tweet, "extended_tweet", nothing)
    if et === nothing
        tweet["text"]
    else
        t = get(et, "full_text", nothing)
        t === nothing ? tweet["text"] : t
    end
end

mentions(text) = [m.match for m in eachmatch(r"(@\w+)", text)]
hashtags(text) = [m.match for m in eachmatch(r"(#\w+)", text)]
urls(text) = [m.match for m in eachmatch(r"(http.?://\S+)", text)]
language(tweet) = tweet["lang"]

function countrycode(tweet)
    place = get(tweet, "place", nothing)
    place === nothing && return nothing
    get(place, "country_code", nothing)
end

function preprocess(text)
    #text = lowercase(text)
    text = replace(text, r"[_\s]+"imx => " ")
    text = replace(text, "&amp;" => "&", "&gt;" => ">", "&lt;" => "<")
    text = replace(text, r"(https?://\S+)" => " _url ")
    text = replace(text, r"#\S+" => " _htag ")
    text = replace(text, r"@\S+" => " _usr ")
    text = replace(text, r"j(a|e|i)[jaei]+"imx => s"j\1j\1")
    replace(text, r"h(a|e|i)[haei]+"imx => s"h\1h\1")
end

function emojis(text_)
    [c for c in text_ if isemoji(c)]
end


function segment_by_countrycode(tweets)
    D = Dict{String,Vector}()
    
    for tweet in tweets
        cc = countrycode(tweet)
        L = get(D, cc, nothing)

        if L === nothing
            D[cc] = [tweet]
        else
            push!(L, tweet)
        end
    end
    
    D    
end

function load_and_segment_by_countrycode(getdata::Function, filenames, lang, validcc)
    D = Dict{String,Vector}()
    
    for filename in filenames
       println(stderr, "loading $filename")
        eachtweet(filename) do tweet
            t = text(tweet)
            cc = countrycode(tweet)
            if !startswith(t, "RT") && language(tweet) == lang && cc in validcc
                L = get(D, cc, nothing)
                data = getdata(tweet)
                if data !== nothing
                    if L === nothing
                        D[cc] = [data]
                    else
                        push!(L, data)
                    end
                end
            end
        end
    end
    
    D
end

load_and_segment_by_countrycode(filenames, lang, validcc) = load_and_segment_by_countrycode(identity, filenames, lang, validcc)

function update!(A, B)
    for (k, v) in B
        append!(A[k], v)
    end
    
    A
end
