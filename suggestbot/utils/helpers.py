from aiogram.types import Message, InputMediaPhoto, InputMediaVideo
from db.queries import get_submission_content


def user_mention(user_id: int, username: str | None, first_name: str) -> str:
    """Возвращает HTML-ссылку на пользователя."""
    name = f"@{username}" if username else first_name
    return f'<a href="tg://user?id={user_id}">{name}</a>'


def format_time(seconds: int) -> str:
    """Форматирует секунды в '4 мин 32 сек'."""
    minutes = seconds // 60
    secs = seconds % 60
    if minutes and secs:
        return f"{minutes} мин {secs} сек"
    elif minutes:
        return f"{minutes} мин"
    else:
        return f"{secs} сек"


async def build_media_input(submission_id: int) -> list:
    """Строит список InputMedia* для отправки медиагруппы."""
    contents = await get_submission_content(submission_id)
    media = []
    for i, c in enumerate(contents):
        caption = c["caption"] if i == 0 else None
        if c["content_type"] == "photo":
            media.append(InputMediaPhoto(media=c["file_id"], caption=caption, parse_mode="HTML"))
        elif c["content_type"] == "video":
            media.append(InputMediaVideo(media=c["file_id"], caption=caption, parse_mode="HTML"))
    return media


def submission_header(sub_id: int, user_id: int, username: str | None, first_name: str) -> str:
    mention = user_mention(user_id, username, first_name)
    return f"📨 <b>Заявка #{sub_id}</b>\nОт: {mention} (<code>{user_id}</code>)\n\n"
