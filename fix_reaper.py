import re

def fix_file(path):
    with open(path, "r") as f:
        content = f.read()

    pattern = r'(await redis\.xack\(\s*settings\.redis_stream_name,\s*settings\.redis_consumer_group,\s*message_id,?\s*\))'
    replacement = r'\1\n                await redis.xdel(settings.redis_stream_name, message_id)'
    
    new_content = re.sub(pattern, replacement, content)
    
    with open(path, "w") as f:
        f.write(new_content)

fix_file("src/fairlane/reaper.py")
