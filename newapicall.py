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

def main():
    videoList = ["https://www.youtube.com/watch?v=GS_VcLRmCoI", "https://www.youtube.com/watch?v=g6eB8IeX_cs", "https://www.youtube.com/watch?v=FvY1fYxKFJU", "https://www.youtube.com/watch?v=DaG-y2tFBFE", "https://www.youtube.com/watch?v=JRl1FhIBeKc", "https://www.youtube.com/watch?v=DPNz6reMVXY", "https://www.youtube.com/watch?v=OvrYfsr4UDI", "https://www.youtube.com/watch?v=LOEXAn9Hj20", "https://www.youtube.com/watch?v=w525cY5FP7k"]
    for video in videoList:
        print(video)
        caps = processTranscript(video)
        print(caps)

main()