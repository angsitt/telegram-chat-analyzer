from telethon import TelegramClient
from credentials import USERNAME, API_ID, API_HASH, dialog_name
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
import pandas as pd

client = TelegramClient(USERNAME, API_ID, API_HASH)

async def get_dialogs_dict() -> dict:
    dialogs = {}
    async for dialog in client.iter_dialogs():
        if dialog.is_user:
            dialog_type = "user"
        elif dialog.is_channel:
            dialog_type = "channel"
        else:
            dialog_type = "group"
        dialogs[dialog.name] = {
            "id": dialog.id,
            "type": dialog_type,
            "unread_msg_cnt": dialog.unread_count,
            "last_msg_dt": str(dialog.date)
        }
    return dialogs

def get_dialog_id(dialogs: dict, dialog_name: str) -> int:
    dialog_id = None
    try:
        dialog_id = dialogs[dialog_name]["id"]
    except:
        print("Dialog name is not found.")
    return dialog_id

async def get_dialog_history(dialog_id: int) -> list:
    messages = []
    attributes = ["msg_id", "username", "msg_text", "msg_dt", "fwd_from_user_id",
                  "is_media", "media_type", "media_duration", "msg_edit_dt",
                  "reply_to_msg_id", "has_reaction", "reaction"]
    try:
        async for msg in client.iter_messages(dialog_id, reverse=True):
            user = msg.from_id if msg.from_id is not None else msg.peer_id
            username = (await client.get_entity(user.user_id)).username
            fwd_user = msg.fwd_from.from_id.user_id if msg.fwd_from is not None else None
            media = True if msg.media is not None else False
            if not media:
                media_type = None
                media_duration = None
            elif isinstance(msg.media, MessageMediaPhoto):
                media_type = "photo"
                media_duration = None
            elif isinstance(msg.media, MessageMediaDocument) and "audio" in msg.media.document.mime_type:
                media_type = "audio"
                media_duration = msg.media.document.attributes[0].duration
            elif isinstance(msg.media, MessageMediaDocument) and "video" in msg.media.document.mime_type:
                media_type = "telegram video" if msg.media.document.attributes[0].round_message else "video"
                media_duration = msg.media.document.attributes[0].duration
            else:
                media_type = type(msg.media)
                media_duration = None
            reply_to_msg = msg.reply_to.reply_to_msg_id if msg.reply_to is not None else None
            reaction = True if msg.reactions is not None else False
            reaction_img = msg.reactions.results[0].reaction if reaction else None
            message = [msg.id, username, msg.message, str(msg.date), fwd_user, media, media_type, media_duration, str(msg.edit_date), reply_to_msg, reaction, reaction_img]
            messages.append(message)
    except:
        print("Dialog id is not found.")
    return (messages, attributes)

def convert_data_to_pd_df(data: list, columns: list) -> pd.DataFrame:
    df = pd.DataFrame(data=data, columns=columns)
    df = df.set_index(columns[0])
    return df

async def main():
    dialogs = await get_dialogs_dict()
    dialog_id = get_dialog_id(dialogs, dialog_name)
    msg_history_data, msg_history_columns = await get_dialog_history(dialog_id)
    df = convert_data_to_pd_df(msg_history_data, msg_history_columns)
    df.to_csv(f"output\{dialog_name.strip()}_chat_history.csv")

with client:
    client.loop.run_until_complete(main())



