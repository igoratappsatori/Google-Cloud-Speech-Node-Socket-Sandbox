'use strict';

//  Google Cloud Speech Playground with node.js and socket.io
//  Created by Vinzenz Aubry for sansho 24.01.17
//  Feel free to improve!
//	Contact: v@vinzenzaubry.com

const express = require('express'); // const bodyParser = require('body-parser'); // const path = require('path');
const environmentVars = require('dotenv').config();

// Google Cloud
const speech = require('@google-cloud/speech');
const speechClient = new speech.SpeechClient(); // Creates a client

const app = express();
const port = 8080;
const server = require('http').createServer(app);

const io = require('socket.io')(server);

app.use('/assets', express.static(__dirname + '/public'));
app.use('/session/assets', express.static(__dirname + '/public'));
app.set('view engine', 'ejs');

let recordingStarted = false;
let streamingLimit = 210000; //210000 ms ~ 3.5 min
let recognizeStream = null;
let restartCounter = 0;
let audioInput = [];
let lastAudioInput = [];
let resultEndTime = 0;
let isFinalEndTime = 0;
let finalRequestEndTime = 0;
let newStream = true;
let bridgingOffset = 0;
let lastTranscriptWasFinal = false;

// =========================== ROUTERS ================================ //

app.get('/', function (req, res) {
  res.render('index', {});
});

app.use('/', function (req, res, next) {
  next(); // console.log(`Request Url: ${req.url}`);
});

// =========================== SOCKET.IO ================================ //

io.on('connection', function (client) {
  console.log('Client Connected to server');

  client.on('join', function () {
    client.emit('messages', 'Socket Connected to Server');
  });

  client.on('startRecording', function () {
    recordingStarted = true;
    io.sockets.emit('recordingSwitched', `${recordingStarted}`);
  });

  client.on('stopRecording', function () {
    recordingStarted = false;
    io.sockets.emit('recordingSwitched', `${recordingStarted}`);
  });

  client.on('messages', function (data) {
    client.emit('broad', data);
  });

  client.on('startGoogleCloudStream', function (data) {
    startRecognitionStream(this, data);
  });

  client.on('endGoogleCloudStream', function () {
    stopRecognitionStream();
  });

  client.on('binaryData', function (data) {
    // console.log(data); //log binary data
    if (recognizeStream !== null) {
      if (newStream && lastAudioInput.length !== 0) {
        // Approximate math to calculate time of chunks
        const chunkTime = streamingLimit / lastAudioInput.length;
        if (chunkTime !== 0) {
          if (bridgingOffset < 0) {
            bridgingOffset = 0;
          }
          if (bridgingOffset > finalRequestEndTime) {
            bridgingOffset = finalRequestEndTime;
          }
          const chunksFromMS = Math.floor(
              (finalRequestEndTime - bridgingOffset) / chunkTime
          );
          bridgingOffset = Math.floor(
              (lastAudioInput.length - chunksFromMS) * chunkTime
          );

          for (let i = chunksFromMS; i < lastAudioInput.length; i++) {
            recognizeStream.write(lastAudioInput[i]);
          }
        }
        newStream = false;
      }

      audioInput.push(data);

      if (recognizeStream) {
        recognizeStream.write(data);
      }
      // recognizeStream.write(data);
    }
  });

  const speechCallback = data => {
    process.stdout.write(
        data.results[0] && data.results[0].alternatives[0]
            ? `Transcription: ${data.results[0].alternatives[0].transcript}\n`
            : '\n\nReached transcription time limit, press Ctrl+C\n'
    );
    io.sockets.emit('speechData', data);
    // console.log(data)
    // client.emit('speechData', data);

    // if end of utterance, let's restart stream
    // this is a small hack. After 65 seconds of silence, the stream will still throw an error for speech length limit
    // if (data.results[0] && data.results[0].isFinal) {
    //   stopRecognitionStream();
    //   startRecognitionStream(client);
    //   // console.log('restarted stream serverside');
    // }
  };


  function startRecognitionStream(client) {
    recognizeStream = speechClient
        .streamingRecognize(request)
        .on('error', console.error)
        .on('data', speechCallback);

    setTimeout(restartStream, streamingLimit);
  }

  function restartStream() {
    if (recognizeStream) {
      recognizeStream.end();
      recognizeStream.removeListener('data', speechCallback);
      recognizeStream = null;
    }
    if (resultEndTime > 0) {
      finalRequestEndTime = isFinalEndTime;
    }
    resultEndTime = 0;

    lastAudioInput = [];
    lastAudioInput = audioInput;

    restartCounter++;

    if (!lastTranscriptWasFinal) {
      process.stdout.write('\n');
    }
    process.stdout.write(
        `${streamingLimit * restartCounter}: RESTARTING REQUEST\n`
    );

    newStream = true;

    startRecognitionStream();
  }

  function stopRecognitionStream() {
    if (recognizeStream) {
      recognizeStream.end();
    }
    recognizeStream = null;
  }
});

// =========================== GOOGLE CLOUD SETTINGS ================================ //

// The encoding of the audio file, e.g. 'LINEAR16'
// The sample rate of the audio file in hertz, e.g. 16000
// The BCP-47 language code to use, e.g. 'en-US'
const encoding = 'LINEAR16';
const sampleRateHertz = 16000;
// const languageCode = 'en-US'; //en-US
const languageCode = 'cs-CZ'; //en-US

const request = {
  config: {
    encoding: encoding,
    sampleRateHertz: sampleRateHertz,
    languageCode: languageCode,
    profanityFilter: false,
    enableWordTimeOffsets: true,
    enableAutomaticPunctuation: true,
    enableWordConfidence: true,
    // enableSpokenPunctuation: true,
    // speechContexts: [{
    //     phrases: ["hoful","shwazil"]
    //    }] // add your own speech context for better recognition
  },
  interimResults: true, // If you want interim results, set this to true
};

// =========================== START SERVER ================================ //

server.listen(port, '127.0.0.1', function () {
  //http listen, to make socket work
  // app.address = "127.0.0.1";
  console.log('Server started on port:' + port);
});
