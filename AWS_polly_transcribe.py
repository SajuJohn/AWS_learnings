import boto3
import time
from urllib.request import urlopen
import json
import subprocess
import string

def Cleanup(transcribe_client):
    """
    Cleanup the transcription job in Amazon Transcribe
    Return: None
    """

    print("\n******** Removing the transcription job: " + transcribe_job_name + " from Amazon Transcribe. **********\n")
    response = transcribe_client.delete_transcription_job(TranscriptionJobName = transcribe_job_name)

def texttoMP3_synthesize():
    """
    Convert a given text to MP3 format using AWS Polly
    Input: None
    Return: URI path of the converted MP3 file.
    """
    
    # Setting the client for Polly
    polly_client = boto3.client('polly')

    # Get the complete set of voices from Polly.
    voice_response = polly_client.describe_voices(LanguageCode='en-US')
    voices_list = voice_response.get('Voices')

    # Search the voice matching Male voice of Matthew
    for voice in voices_list:
        print('Gender: ' + voice.get('Gender') + '\tName: ' + voice.get('Name'))
        if (voice.get('Gender') == 'Male' and voice.get('Name') == 'Matthew'):
            print("Found the required voice for this assignment!!!\n")        
            break
        else:
            print("Didnot find the required voice.\n")
        
    print("----------------- Starting Speech Synthesis Task ----------------\n")
    # Create a mp3 file and store in S3 bucket. The text to synthesize is "The sun does arise and make happy the skies.
    # The merry bells ring to welcome the spring." using Matthew's voice.
    
    synthesis_response = polly_client.start_speech_synthesis_task(LanguageCode = 'en-US',
                                                                  OutputFormat = 'mp3',
                                                                  OutputS3BucketName = bucket_name,
                                                                  Text = text_to_synthesize,
                                                                  VoiceId = 'Matthew')

    synthesis_taskid = synthesis_response['SynthesisTask']['TaskId']
    # Loop through till transcription job gets completed.
    print('Waiting for job to complete...')
    while True:
        response = polly_client.get_speech_synthesis_task(TaskId = synthesis_taskid)
        if response['SynthesisTask']['TaskStatus'] in ['completed', 'failed']:
            if (response['SynthesisTask']['TaskStatus'] == 'failed'):
                print("MP3 conversion FAILED!!!. TaskStatusReason: " + synthesis_status.get('TaskStatusReason') + '... Exiting !!!')
                exit()
            break
        else:
            print('Still waiting... for 5 secs ...')
        time.sleep(5)

    print("Text to MP3 converted successfully. \nPlease find the URI path: " + response['SynthesisTask']['OutputUri'])
    uri_path = response['SynthesisTask']['OutputUri']

    return uri_path

def MP3totext_transcribe(uri_path):
    """
    Convert a MP3 file to text format using AWS Transcribe
    Input: URI path of the converted MP3 file.
    Return: URI path of the converted text file.
            transcribe client handle.
    """

    # Setting the client for Transcribe
    transcribe_client = boto3.client('transcribe')    

    print("\n----------------- Now Starting Transcription Job ----------------\n")
    # Transcribe the previously created MP3 file to text format and store in the same S3 bucket.
    # Note: TranscriptionJobName has to be unique and so has to be created each time we re-execute this file.
    transcription_response = transcribe_client.start_transcription_job(TranscriptionJobName = transcribe_job_name,
                                                                       LanguageCode = 'en-US',
                                                                       MediaFormat='mp3',
                                                                       Media = {'MediaFileUri' : uri_path},
                                                                       OutputBucketName = bucket_name)

    # Loop through till transcription job gets completed.
    print('Waiting for job to complete...')
    while True:
        response = transcribe_client.get_transcription_job(TranscriptionJobName = transcribe_job_name)
        if response['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            if (response['TranscriptionJob']['TranscriptionJobStatus'] == 'FAILED'):
                print("MP3 transcription FAILED !!!. TaskStatusReason: " + response['TranscriptionJob']['FailureReason'])
            break
        else:
            print('Still waiting... for 5 secs ...')
        time.sleep(5)

    transcript_uri = response['TranscriptionJob']['Transcript']['TranscriptFileUri']    
    print("MP3 to Text transcription done successfully. \nPlease find the URI path: " + transcript_uri)

    return transcript_uri, transcribe_client


def grant_access():
    """
    Grant access to a file in S3 bucket using AWS S3. Using AWS command CLI for this purpose. Bucket and object key and the type of access is defined outside.
    Input: None.
    Return: None.            
    """
    # Note: For accessing the transcript URL, you will need PUBLIC ACCESS. The below set of JSON code worked after providing PUBLIC ACCESS to "polly-mp3-assignment-bucket"
    #       Before you use a bucket policy to grant read-only permission to an anonymous user, you must disable block public access settings for your bucket.

    command = 'aws.bat s3api put-object-acl --bucket ' + bucket_name + ' --key ' + object_key + ' --acl ' + access_type
    result = subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode != 0:
        print('Changing object access using put-object-acl command Failed. Reason: ' + result.stderr)
        exit();

def verify_transcript(transcript_uri):
    """
    Loads the text file and prints its contents.
    Input: URI path of the converted text file.
    Return: None.            
    """

    # Open the JSON file, read it, and get the transcript.
    response = urlopen(transcript_uri)
    raw_json = response.read()
    loaded_json = json.loads(raw_json)
    transcript = loaded_json['results']['transcripts'][0]['transcript']

    print("\n************** Finally please see the input and output texts below ****************\n")
    print("Text Input file before MP3 conversion: " + text_to_synthesize)
    print("Transcript form after MP3 conversion: " + transcript)

    
print("\n\n########################### Assignment Started. #######################################\n\n")

text_to_synthesize = "The sun does arise and make happy the sky's. The merry bells ring to welcome the spring."
bucket_name = 'polly-mp3-assignment-bucket'
object_key = 'mp3_transcribe_to_text.json'
access_type = 'public-read'
transcribe_job_name = 'mp3_transcribe_to_text'

uri_path = texttoMP3_synthesize()
transcript_uri, transcribe_client = MP3totext_transcribe(uri_path)
grant_access()
verify_transcript(transcript_uri)
Cleanup(transcribe_client)

print("\n\n########################### Assignment Completed. #######################################\n\n")
