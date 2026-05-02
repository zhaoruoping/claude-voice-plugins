#!/usr/bin/env python3
"""TTS synthesis with swappable backends.

Usage:
    python synthesize.py "要合成的文字" output.mp3
    python synthesize.py --provider minimax "要合成的文字" output.mp3
    python synthesize.py --voice "female-yujie" "文字" output.mp3
"""
import sys
import os
import json
import subprocess
import tempfile

# Load .env from script directory
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())


class MiniMaxProvider:
    """TTS using MiniMax Speech API."""

    API_URL = "https://api.minimax.chat/v1/t2a_v2"

    # Common voice IDs
    VOICES = {
        "male-qn-qingse": "青涩青年音色",
        "female-shaonv": "少女音色",
        "female-yujie": "御姐音色",
        "male-qn-jingying": "精英青年音色",
        "presenter_male": "男性主持人",
        "presenter_female": "女性主持人",
        "audiobook_male_1": "男性有声书1",
        "audiobook_female_1": "女性有声书1",
    }

    @staticmethod
    def synthesize(text: str, output_path: str, voice_id: str = "presenter_male",
                   speed: float = 1.0) -> bool:
        import httpx

        api_key = os.environ.get("MINIMAX_API_KEY", "")
        group_id = os.environ.get("MINIMAX_GROUP_ID", "")

        if not api_key:
            print("Error: MINIMAX_API_KEY not set", file=sys.stderr)
            return False

        url = MiniMaxProvider.API_URL
        if group_id:
            url += f"?GroupId={group_id}"

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": "speech-02-turbo",
            "text": text,
            "stream": False,
            "voice_setting": {
                "voice_id": voice_id,
                "speed": speed,
                "vol": 1.0,
                "pitch": 0,
            },
            "audio_setting": {
                "sample_rate": 32000,
                "bitrate": 128000,
                "format": "mp3",
            },
        }

        try:
            with httpx.Client(timeout=60.0) as client:
                resp = client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()

                if "data" in data and "audio" in data["data"]:
                    # MiniMax returns hex-encoded audio
                    audio_bytes = bytes.fromhex(data["data"]["audio"])
                    with open(output_path, "wb") as f:
                        f.write(audio_bytes)
                    return True
                elif "base_resp" in data:
                    print(f"API error: {data['base_resp']}", file=sys.stderr)
                    return False
                else:
                    # Maybe the response itself is audio
                    print(f"Unexpected response format: {json.dumps(data, ensure_ascii=False)[:500]}", file=sys.stderr)
                    return False
        except httpx.HTTPStatusError as e:
            print(f"HTTP error {e.response.status_code}: {e.response.text[:500]}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return False


def convert_to_ogg_opus(input_path: str, output_path: str = None) -> str:
    """Convert audio to OGG Opus format for Telegram voice messages."""
    if output_path is None:
        output_path = os.path.splitext(input_path)[0] + ".ogg"
    subprocess.run(
        ["ffmpeg", "-y", "-i", input_path, "-c:a", "libopus", "-b:a", "64k",
         "-ar", "48000", "-ac", "1", output_path],
        capture_output=True, check=True
    )
    return output_path


def synthesize(text: str, output_path: str, provider: str = "minimax",
               voice: str = "presenter_male", speed: float = 1.0) -> bool:
    """Synthesize text to audio file.

    Args:
        text: Text to synthesize
        output_path: Output audio file path
        provider: TTS provider name
        voice: Voice ID
        speed: Speech speed (0.5-2.0)

    Returns:
        True if successful
    """
    if provider == "minimax":
        return MiniMaxProvider.synthesize(text, output_path, voice, speed)
    else:
        raise ValueError(f"Unknown TTS provider: {provider}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Synthesize text to speech")
    parser.add_argument("text", help="Text to synthesize")
    parser.add_argument("output", help="Output audio file path")
    parser.add_argument("--provider", default="minimax", help="TTS provider (default: minimax)")
    parser.add_argument("--voice", default="presenter_male", help="Voice ID")
    parser.add_argument("--speed", type=float, default=1.0, help="Speech speed")
    parser.add_argument("--ogg", action="store_true", help="Also convert to OGG Opus for Telegram")
    parser.add_argument("--list-voices", action="store_true", help="List available voices")
    args = parser.parse_args()

    if args.list_voices:
        print("MiniMax voices:")
        for vid, desc in MiniMaxProvider.VOICES.items():
            print(f"  {vid:30s} {desc}")
        sys.exit(0)

    ok = synthesize(args.text, args.output, args.provider, args.voice, args.speed)
    if ok:
        print(f"Saved to: {args.output}")
        if args.ogg:
            ogg_path = convert_to_ogg_opus(args.output)
            print(f"OGG Opus: {ogg_path}")
    else:
        print("Synthesis failed", file=sys.stderr)
        sys.exit(1)
