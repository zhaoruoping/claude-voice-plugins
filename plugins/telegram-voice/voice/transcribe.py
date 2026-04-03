#!/usr/bin/env python3
"""STT transcription with swappable backends.

Usage:
    python transcribe.py <audio_file>
    python transcribe.py --provider funasr <audio_file>
    python transcribe.py --provider dashscope <audio_file>
"""
import sys
import os
import subprocess
import json
import tempfile
import base64

# Load .env from script directory
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())


def convert_to_wav(input_path: str) -> str:
    """Convert any audio format to 16kHz mono WAV using ffmpeg."""
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    subprocess.run(
        ["ffmpeg", "-y", "-i", input_path, "-ar", "16000", "-ac", "1", tmp.name],
        capture_output=True, check=True
    )
    return tmp.name


class FunASRProvider:
    """Local STT using FunASR SenseVoice (GPU/CPU, free, fast)."""

    _model = None

    @classmethod
    def get_model(cls):
        if cls._model is None:
            from funasr import AutoModel
            cls._model = AutoModel(
                model="iic/SenseVoiceSmall",
                trust_remote_code=True,
                vad_model="fsmn-vad",
                vad_kwargs={"max_single_segment_time": 30000},
                device="cuda:0",
            )
        return cls._model

    @staticmethod
    def transcribe(audio_path: str) -> str:
        from funasr.utils.postprocess_utils import rich_transcription_postprocess
        model = FunASRProvider.get_model()
        res = model.generate(
            input=audio_path,
            cache={},
            language="auto",
            use_itn=True,
            batch_size_s=300,
        )
        if res and len(res) > 0:
            return rich_transcription_postprocess(res[0]["text"])
        return ""


class DashScopeProvider:
    """Cloud STT using Alibaba DashScope Qwen3-ASR (OpenAI-compatible)."""

    @staticmethod
    def transcribe(audio_path: str) -> str:
        from openai import OpenAI

        api_key = os.environ.get("DASHSCOPE_API_KEY", "")
        if not api_key:
            raise ValueError("DASHSCOPE_API_KEY not set in .env")

        client = OpenAI(
            api_key=api_key,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
        )

        with open(audio_path, "rb") as f:
            audio_b64 = base64.b64encode(f.read()).decode()

        # Detect MIME type from extension
        ext = os.path.splitext(audio_path)[1].lower()
        mime_map = {".ogg": "audio/ogg", ".oga": "audio/ogg", ".wav": "audio/wav",
                    ".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".flac": "audio/flac"}
        mime = mime_map.get(ext, "audio/ogg")

        response = client.chat.completions.create(
            model="qwen3-asr-flash",
            messages=[{
                "role": "user",
                "content": [{
                    "type": "input_audio",
                    "input_audio": {
                        "data": f"data:{mime};base64,{audio_b64}"
                    }
                }]
            }],
            extra_body={"asr_options": {"language": "zh"}}
        )
        return response.choices[0].message.content or ""


def transcribe(audio_path: str, provider: str = "funasr") -> str:
    """Transcribe audio file to text.

    Args:
        audio_path: Path to audio file (any format ffmpeg supports)
        provider: STT provider name (funasr or dashscope)

    Returns:
        Transcribed text
    """
    if provider == "dashscope":
        # DashScope accepts OGG/WAV/MP3 directly, no conversion needed
        return DashScopeProvider.transcribe(audio_path)

    # Local providers need WAV conversion
    ext = os.path.splitext(audio_path)[1].lower()
    if ext not in (".wav",):
        wav_path = convert_to_wav(audio_path)
        cleanup = True
    else:
        wav_path = audio_path
        cleanup = False

    try:
        if provider == "funasr":
            text = FunASRProvider.transcribe(wav_path)
        else:
            raise ValueError(f"Unknown STT provider: {provider}")
    finally:
        if cleanup and os.path.exists(wav_path):
            os.unlink(wav_path)

    return text


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Transcribe audio to text")
    parser.add_argument("audio_file", help="Path to audio file")
    parser.add_argument("--provider", default="funasr",
                        help="STT provider: funasr (local GPU) or dashscope (cloud)")
    args = parser.parse_args()

    if not os.path.exists(args.audio_file):
        print(f"Error: File not found: {args.audio_file}", file=sys.stderr)
        sys.exit(1)

    text = transcribe(args.audio_file, args.provider)
    print(text)
