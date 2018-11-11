from youtube_transcript_api import YouTubeTranscriptApi

# takes a youtube url and provides a long-ass string with all of the captions in it
def processTranscript(youtubeUrl):
    videoID = youtubeUrl.split('v=')[1] #splits the url and selects the video id
    out = YouTubeTranscriptApi.get_transcript(videoID)
    array = out
    captions = ""

    for val in array:
        captions += ' ' + val.get('text')
        
    return(captions)
        
print(processTranscript("https://www.youtube.com/watch?v=HY8HQUlepE0"))