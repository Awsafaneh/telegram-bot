import re, os
import logging
import io
import datetime
import asyncio # For running blocking code in thread
from urllib.parse import urlparse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, constants, Message, InputFile
import telegram.error
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, CallbackQueryHandler, ConversationHandler, ApplicationBuilder
)

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.ExtBot").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# --- Constants ---
TELEGRAM_BOT_TOKEN = "7870088512:AAH-qxaggs3snu6g_p5XT_bxv1czkZTHy1E" # Replace with your bot token
DEFAULT_COMBO_FILE = "data6.txt"
BOT_DEVELOPER_NAME = "AWS AFANEH"
INSTAGRAM_URL = "https://instagram.com/tech_by_aws" # Updated URL

# --- Global Variables / Initial Setup ---
COMBO_LINES: list[str] = []
COMBO_LOAD_SUCCESS: bool = False
SCRIPT_DIR: str | None = None
FULL_COMBO_PATH: str | None = None

# --- Conversation States (Modes 1, 2, 3 only) ---
SELECTING_MODE, \
AWAITING_SINGLE_SITE, AWAITING_LIMIT_M1, \
AWAITING_LIMIT_M2, \
AWAITING_SEARCH_TERM_M3, AWAITING_LIMIT_M3 = range(6)

# --- Predefined site list (for Mode 2 - uses exact domain matching) ---
PREDEFINED_SITES = [
    'instagram.com', 'facebook.com', 'snapchat.com', 'twitter.com', 'google.com', 'discord.com',
    'roblox.com', 'netflix.com', 'shahid.net', 'tiktok.com', 'talabat.com', 'apple.com', 'paypal.com',
    'amazon.com', 'idmsa.apple.com', 'twitch.tv', 'skaraudio.com', 'shein.com', 'stake.com', 'callofduty.com',
    'sony.com', 'epicgames.com', '2captcha.com', 'roobet.com', 'godaddy.com', 'pythonanywhere.com',
    '1xbet.com', 'outlook.com', 'crunchyroll.com'
]

# --- Helper Functions ---

def escape_markdown_v2(text: str) -> str:
    """Escapes reserved MarkdownV2 characters in a given string."""
    if not isinstance(text, str): text = str(text)
    escape_chars = r'_*\[\]()~`>#+-=|{}.!'
    text = text.replace('\\', '\\\\')
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

async def safe_reply(chat_id: int | None, context: ContextTypes.DEFAULT_TYPE, text: str, store_message: bool = False, **kwargs) -> Message | None:
    """
    Safely sends a message using MarkdownV2. Handles escaping internally.
    Optionally stores reference to the sent message in user_data['active_prompt_message'].
    Returns sent Message or None.
    """
    if not chat_id:
        logger.warning(f"safe_reply cannot send message: No chat_id provided.")
        return None

    sent_message: Message | None = None
    escaped_text = escape_markdown_v2(text) # Escape the raw input text

    try:
        kwargs.pop('parse_mode', None)
        bot = context.bot
        sent_message = await bot.send_message(
            chat_id=chat_id,
            text=escaped_text,
            parse_mode=constants.ParseMode.MARKDOWN_V2,
            **kwargs
        )
        if store_message and context.user_data is not None and sent_message:
            context.user_data['active_prompt_message'] = sent_message

    except telegram.error.BadRequest as e:
        logger.warning(f"MarkdownV2 reply failed despite escaping: {e}. Raw Text: '{text[:150]}...' Escaped: '{escaped_text[:150]}...' Retrying plain.")
        sent_message = await _retry_plain_text(chat_id, context, text, **kwargs) # Pass original raw text
        if store_message and context.user_data is not None and sent_message:
             context.user_data['active_prompt_message'] = sent_message
    except telegram.error.Forbidden as e:
         logger.error(f"Bot forbidden from sending message to chat {chat_id}: {e}")
    except telegram.error.TelegramError as e:
        logger.error(f"Telegram error during reply to {chat_id}: {e}. Raw Text: '{text[:100]}...'")
    except Exception as e:
        logger.error(f"Unexpected error during safe_reply to {chat_id}: {e}. Raw Text: '{text[:100]}...'", exc_info=True)
        sent_message = await _retry_plain_text(chat_id, context, "⚠️ An internal error occurred. Please try again later.", **kwargs)
        if store_message and context.user_data is not None and sent_message:
            context.user_data['active_prompt_message'] = sent_message

    return sent_message

async def _retry_plain_text(chat_id: int, context: ContextTypes.DEFAULT_TYPE, raw_text: str, **kwargs) -> Message | None:
    """Internal helper to retry sending as plain text. Takes RAW text."""
    sent_message: Message | None = None
    try:
        kwargs.pop('parse_mode', None)
        kwargs.pop('reply_markup', None)
        plain_text = raw_text[:4096] if len(raw_text) > 4096 else raw_text
        sent_message = await context.bot.send_message(chat_id=chat_id, text=plain_text, **kwargs)
    except Exception as e_plain:
        logger.error(f"Plain text reply also failed to chat {chat_id}. Error: {e_plain}. Raw Text: '{raw_text[:100]}...'")
    return sent_message

async def safe_edit_message(message: Message | None, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs) -> bool:
    """
    Safely edits a given message using MarkdownV2. Handles escaping internally.
    Returns True on success/no change, False on error.
    """
    if not message:
        logger.warning("safe_edit_message called with None message object.")
        return False

    edited = False
    message_id = message.message_id
    chat_id = message.chat_id
    escaped_text = escape_markdown_v2(text) # Escape the raw input text

    try:
        kwargs.pop('parse_mode', None)
        await message.edit_text(
            text=escaped_text,
            parse_mode=constants.ParseMode.MARKDOWN_V2,
            **kwargs
        )
        edited = True

    except telegram.error.BadRequest as e:
        if "Message is not modified" in str(e).lower():
            logger.info(f"Message {message_id} not modified, skipping edit.")
            edited = True
        else:
            logger.warning(f"Failed to edit message {message_id} (BadRequest): {e}. Raw Text: '{text[:150]}...' Escaped: '{escaped_text[:150]}...'")
            edited = False
    except telegram.error.TelegramError as e:
        logger.error(f"Telegram error during edit of message {message_id}: {e}")
        edited = False
    except Exception as e:
        logger.error(f"Unexpected error during safe_edit_message for message {message_id}: {e}", exc_info=True)
        edited = False
    return edited

async def _delete_message_safely(message: Message | None):
    """Tries to delete a message, logs errors."""
    if message:
        try:
            await message.delete()
            logger.debug(f"Deleted message {message.id}")
        except telegram.error.BadRequest as e:
             error_str = str(e).lower()
             if "message to delete not found" in error_str or "message can't be deleted" in error_str:
                  logger.debug(f"Message {message.id} already deleted or cannot be deleted.")
             else:
                  logger.warning(f"Failed to delete message {message.id} (BadRequest): {e}")
        except Exception as e:
            logger.warning(f"Could not delete message {message.id}: {e}")

def load_combo_file() -> None:
    """Loads combo data, sets COMBO_LOAD_SUCCESS flag."""
    global COMBO_LINES, COMBO_LOAD_SUCCESS
    COMBO_LINES = []
    COMBO_LOAD_SUCCESS = False
    if not FULL_COMBO_PATH or not os.path.exists(FULL_COMBO_PATH):
        logger.error(f"CRITICAL: Combo file path invalid or not found: '{FULL_COMBO_PATH}'. Modes 1-3 unavailable.")
        return
    try:
        with open(FULL_COMBO_PATH, 'r', encoding='utf-8', errors='ignore') as f:
            COMBO_LINES = [line.strip() for line in f if line.strip()]
        if COMBO_LINES:
            logger.info(f"Successfully loaded {len(COMBO_LINES):,} lines from {DEFAULT_COMBO_FILE}.")
            COMBO_LOAD_SUCCESS = True
        else:
             logger.warning(f"Combo file '{DEFAULT_COMBO_FILE}' loaded but empty. Modes 1-3 may yield no results.")
             COMBO_LOAD_SUCCESS = True
    except Exception as e:
        logger.error(f"Failed to load combo file {FULL_COMBO_PATH}: {e}", exc_info=True)

# --- Data Processing Functions (Blocking - use with asyncio.to_thread) ---

def process_combo_filter_domain(target_domain: str, limit: int | None) -> list[str]:
    """
    Filters COMBO_LINES for lines containing the base name of the target domain
    (e.g., 'instagram' from 'instagram.com') case-insensitively.
    """
    if not COMBO_LINES: return []
    results = []
    # Extract base name (part before first dot)
    base_name = target_domain.split('.', 1)[0].lower()
    if not base_name:
        logger.warning(f"Could not extract base name from target domain: {target_domain}")
        return []

    logger.info(f"Filtering broadly for base name: '{base_name}'")
    count = 0
    for line in COMBO_LINES:
        if base_name in line.lower():
            results.append(line)
            count += 1
            if limit is not None and count >= limit:
                break
    return results

def process_combo_filter_common(target_domains: list[str], limit: int | None) -> list[str]:
    """Filters COMBO_LINES for lines containing any of the exact common target domains."""
    if not COMBO_LINES: return []
    results = []
    escaped_domains = [re.escape(d.lower()) for d in target_domains]
    domain_pattern = "|".join(escaped_domains)
    pattern = re.compile(rf"(?:^|[@:/])({domain_pattern})([:\s]|$)", re.IGNORECASE)
    count = 0
    for line in COMBO_LINES:
        if pattern.search(line):
             results.append(line)
             count += 1
             if limit is not None and count >= limit:
                 break
    return results

def process_combo_search(search_term: str, limit: int | None) -> list[str]:
    """Searches COMBO_LINES for term (case-insensitive)."""
    if not COMBO_LINES: return []
    results = []
    term_lower = search_term.lower()
    count = 0
    for line in COMBO_LINES:
        if term_lower in line.lower():
            results.append(line)
            count += 1
            if limit is not None and count >= limit:
                break
    return results

# --- Result Sending Function ---

async def send_results_as_file(chat_id: int, context: ContextTypes.DEFAULT_TYPE,
                             results: list[str], base_filename: str, caption_prefix: str):
    """Sends results as a .txt file and the 'Start Again' prompt."""
    file_sent = False
    num_results = len(results)

    if not results:
        await safe_reply(chat_id, context, "ℹ️ No matching results found.")
    else:
        processing_msg_text = f"⚙️ Preparing file with {num_results:,} result{'s' if num_results != 1 else ''}..."
        preparing_msg = await safe_reply(chat_id, context, processing_msg_text)

        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
            safe_base = re.sub(r'[^\w\-]+', '_', base_filename)[:50]
            filename = f"{safe_base}_{timestamp}.txt"
            raw_caption = f"{caption_prefix} ({num_results:,} {'line' if num_results == 1 else 'lines'} found)."
            escaped_caption = escape_markdown_v2(raw_caption) # Escape the whole caption

            def create_file_content():
                content = "\n".join(results)
                content += f"\n\n# --- Results: {num_results:,} --- #\n"
                content += f"# Bot by: {BOT_DEVELOPER_NAME} --- #"
                return content

            file_content = await asyncio.to_thread(create_file_content)
            file_bio = io.BytesIO(file_content.encode('utf-8'))
            file_bio.name = filename

            await context.bot.send_document(
                chat_id=chat_id,
                document=InputFile(file_bio, filename=filename),
                caption=escaped_caption, # Send the escaped caption
                parse_mode=constants.ParseMode.MARKDOWN_V2 # Ensure parse mode is set
            )
            file_sent = True
            logger.info(f"Sent {filename} ({num_results:,} lines) to chat {chat_id}")

        except telegram.error.TelegramError as e:
            logger.error(f"Failed to send file {base_filename} to chat {chat_id}: {e}", exc_info=True)
            await safe_reply(chat_id, context, f"⚠️ Error sending results file: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error creating/sending file {base_filename} for chat {chat_id}: {e}", exc_info=True)
            await safe_reply(chat_id, context, "⚠️ An unexpected error occurred preparing the results file.")
        finally:
            if preparing_msg:
                await _delete_message_safely(preparing_msg)

    # Send 'Start Again' prompt (raw text passed to safe_reply)
    keyboard = [[InlineKeyboardButton("🔄 Start Again", callback_data="start_again")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if file_sent:
        prompt_text = "✅ File sent successfully!"
    elif not results:
        prompt_text = "ℹ️ Operation finished."
    else:
        prompt_text = f"⚠️ Operation finished, but the results file could not be sent."
    prompt_text += "\nWould you like to perform another operation?"
    await safe_reply(chat_id, context, prompt_text, reply_markup=reply_markup)


# --- Bot Command Handlers & Conversation Steps ---

async def show_main_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, user_first_name: str | None = "User"):
    """Sends the main menu with buttons."""
    keyboard = [
        [InlineKeyboardButton("🌐 Filter by Site Name", callback_data="mode_1")],
        [InlineKeyboardButton("✨ Filter Common Sites", callback_data="mode_2")],
        [InlineKeyboardButton("🔍 Search All Data", callback_data="mode_3")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Construct raw text for the welcome message
    user_name_display = user_first_name or "User" # Fallback
    # Display URL as plain text for reliability
    developer_info = f"Bot by {BOT_DEVELOPER_NAME} (Instagram: {INSTAGRAM_URL})"

    welcome_lines = [
        f"👋 Welcome back, *{user_name_display}*!", # Use markdown directly
        "\n👇 Choose an operation:",
        "---",
        f"_{developer_info}_" # Italicize the developer info line
    ]
    if not COMBO_LOAD_SUCCESS and DEFAULT_COMBO_FILE:
        warning = f"\n\n⚠️ *Note:* Data file (`{DEFAULT_COMBO_FILE}`) unavailable. Modes 1, 2, and 3 may not work."
        welcome_lines.insert(2, warning)

    welcome_text = "\n".join(welcome_lines)
    # Pass raw text, safe_reply handles escaping
    await safe_reply(chat_id, context, welcome_text, reply_markup=reply_markup, store_message=True)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts conversation, shows one-time intro OR main menu."""
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user or not chat_id: return ConversationHandler.END

    logger.info(f"User {user.id} ({user.username or 'NoUsername'}) started/restarted in chat {chat_id}.")

    # Initialize user_data if it doesn't exist
    if context.user_data is None:
        context.user_data = {'chat_id': chat_id, 'has_seen_intro': False}
    has_seen_intro = context.user_data.get('has_seen_intro', False)

    # --- Preserve essential data before potentially clearing ---
    preserved_data = {
        'chat_id': chat_id,
        'has_seen_intro': has_seen_intro,
        # Store user's first name for potential use in start_again -> show_main_menu
        'first_name': user.first_name
    }

    # Clear previous conversation state, but keep preserved data
    keys_to_clear = ['mode', 'domain', 'sites', 'search_term', 'active_prompt_message']
    for key in keys_to_clear:
        context.user_data.pop(key, None)
    # Ensure preserved data overwrites any potential remnants if keys overlap
    context.user_data.update(preserved_data)


    if not has_seen_intro:
        logger.info(f"Showing one-time intro to user {user.id}")
        intro_text = (
            "🔔 *Subscription Required* 🔔\n\n"
            "To use this bot, please subscribe by contacting the developer on Instagram:\n"
            f"➡️ {INSTAGRAM_URL}\n\n" # Plain URL
            "After subscribing, use /start again."
        )
        await safe_reply(chat_id, context, intro_text)
        context.user_data['has_seen_intro'] = True # Mark as seen
        return ConversationHandler.END
    else:
        # Show main menu directly
        await show_main_menu(chat_id, context, user.first_name)
        return SELECTING_MODE


async def start_again(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles 'Start Again' button: deletes prompt, shows main menu."""
    query = update.callback_query
    if not query or not query.message:
        logger.warning("start_again called without query/message.")
        return ConversationHandler.END # Cannot proceed reliably

    chat_id = query.message.chat_id
    await query.answer()
    await _delete_message_safely(query.message) # Delete the message with the button

    # Retrieve stored first name if available
    user_first_name = context.user_data.get('first_name', "User")

    # Show the main menu
    await show_main_menu(chat_id, context, user_first_name)
    return SELECTING_MODE # Return to the first state

async def select_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles mode selection, checks data, edits menu message to first prompt."""
    query = update.callback_query
    menu_message = context.user_data.get('active_prompt_message')
    if not query or not menu_message:
        logger.warning("select_mode called without query or active prompt message.")
        if update.effective_chat:
            await safe_reply(update.effective_chat.id, context, "An error occurred. Please /start again.")
        return ConversationHandler.END

    await query.answer()
    mode = query.data
    user = update.effective_user
    chat_id = query.message.chat_id
    logger.info(f"User {user.id} selected mode: {mode}")
    context.user_data['mode'] = mode

    if mode in ["mode_1", "mode_2", "mode_3"] and not COMBO_LOAD_SUCCESS:
         err_text = f"⚠️ *Mode Unavailable*\n\nData file (`{DEFAULT_COMBO_FILE}`) couldn't load. Modes 1-3 need this data."
         await safe_edit_message(menu_message, context, err_text)
         context.user_data.pop('active_prompt_message', None)
         return ConversationHandler.END

    prompt_text = ""
    next_state = SELECTING_MODE

    cancel_button = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data='cancel')]])

    if mode == "mode_1":
        prompt_text = "🌐 **Mode 1: Filter by Site Name**\n\nPlease enter the site name (e.g., `instagram`, `netflix`). I will search for lines containing this name."
        next_state = AWAITING_SINGLE_SITE
    elif mode == "mode_2":
        context.user_data['sites'] = PREDEFINED_SITES
        prompt_text = f"✨ **Mode 2: Filter by Common Sites**\nFiltering for *{len(PREDEFINED_SITES)}* common domains. \n\n🔢 Enter the maximum results (number or `all`):"
        next_state = AWAITING_LIMIT_M2
    elif mode == "mode_3":
        prompt_text = "🔍 **Mode 3: Search All Data**\n\nEnter the Email, Username, or any text to search for within the data lines (case-insensitive)."
        next_state = AWAITING_SEARCH_TERM_M3
    else:
        logger.error(f"Unexpected mode selected: {mode}")
        await safe_edit_message(menu_message, context, "⚠️ Invalid selection.")
        await start(update, context) # Call start to reset cleanly
        return ConversationHandler.END # End current instance, start handles state

    edited = await safe_edit_message(menu_message, context, prompt_text, reply_markup=cancel_button)
    if not edited:
         logger.warning(f"Failed to edit menu message for mode {mode}, sending new prompt.")
         await safe_reply(chat_id, context, prompt_text, reply_markup=cancel_button, store_message=True)
         # If edit failed, the old menu message might still exist, try deleting it
         await _delete_message_safely(menu_message)

    return next_state

# --- Mode 1 Handlers ---
async def handle_site_input_m1(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles site name input, asks for limit."""
    message = update.message
    site_name_input = message.text.strip().lower() if message and message.text else ""
    chat_id = update.effective_chat.id
    prompt_message = context.user_data.get('active_prompt_message')

    await _delete_message_safely(message)

    if not site_name_input:
        error_text = "⚠️ Site name cannot be empty. Please enter a name (e.g., `instagram`)."
        if prompt_message:
            cancel_button = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data='cancel')]])
            full_error_prompt = f"🌐 **Mode 1: Filter by Site Name**\n\n*{error_text}*\nPlease try again:"
            await safe_edit_message(prompt_message, context, full_error_prompt, reply_markup=cancel_button)
        else:
             await safe_reply(chat_id, context, error_text + "\nPlease try again:")
        return AWAITING_SINGLE_SITE

    context.user_data['domain'] = site_name_input # Use 'domain' key for simplicity
    logger.info(f"Mode 1: User {update.effective_user.id} entered site name: {site_name_input}")

    await _delete_message_safely(context.user_data.pop('active_prompt_message', None))

    limit_prompt_text = f"✅ Searching for lines containing: `{site_name_input}`\n🔢 Enter the maximum results (number or `all`):"
    cancel_button = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data='cancel')]])
    await safe_reply(chat_id, context, limit_prompt_text, reply_markup=cancel_button, store_message=True)

    return AWAITING_LIMIT_M1

async def handle_limit_m1(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles limit input for Mode 1. Processes using broad search."""
    limit_prompt_message = context.user_data.get('active_prompt_message')
    chat_id = update.effective_chat.id

    limit_value = await parse_limit_input_and_edit_on_error(update, context, limit_prompt_message)
    if limit_value is None: return AWAITING_LIMIT_M1

    await _delete_message_safely(update.message)
    await _delete_message_safely(limit_prompt_message)
    context.user_data.pop('active_prompt_message', None)

    target_site_name = context.user_data.get('domain') # Stored site name input
    if not target_site_name:
        logger.error(f"Mode 1 Error: Site name missing for user {update.effective_user.id}")
        await safe_reply(chat_id, context, "⚠️ Error: Internal data lost (Site Name). Please /start again.")
        return ConversationHandler.END

    limit_display = "all" if limit_value == 0 else f"{limit_value:,}"
    processing_text = f"⚙️ Processing: Filtering for lines containing `{target_site_name}` ({limit_display} results)..."
    processing_msg = await safe_reply(chat_id, context, processing_text)

    results = []
    try:
        # Call the filter function (now doing broad search based on site name)
        results = await asyncio.to_thread(
            process_combo_filter_domain, target_site_name, limit_value if limit_value > 0 else None
        )
        logger.info(f"Mode 1: Found {len(results)} results for site name '{target_site_name}' (limit: {limit_display})")
    except Exception as e:
         logger.error(f"Error filtering (Mode 1, site name: {target_site_name}): {e}", exc_info=True)
         await safe_reply(chat_id, context, f"⚠️ Error during filtering: {str(e)}")
         await send_results_as_file(chat_id, context, [], f"M1_Filter_{target_site_name}", "Error")
         return ConversationHandler.END
    finally:
         if processing_msg: await _delete_message_safely(processing_msg)

    await send_results_as_file(chat_id, context, results,
                               f"M1_Filter_{target_site_name}",
                               f"📊 Mode 1 Results containing '{target_site_name}'")
    return ConversationHandler.END

# --- Mode 2 Handlers ---
async def handle_limit_m2(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles limit input for Mode 2. Processes using exact common domain match."""
    limit_prompt_message = context.user_data.get('active_prompt_message')
    chat_id = update.effective_chat.id

    limit_value = await parse_limit_input_and_edit_on_error(update, context, limit_prompt_message)
    if limit_value is None: return AWAITING_LIMIT_M2

    await _delete_message_safely(update.message)
    await _delete_message_safely(limit_prompt_message)
    context.user_data.pop('active_prompt_message', None)

    target_sites = context.user_data.get('sites')
    if not target_sites or target_sites != PREDEFINED_SITES:
        logger.error(f"Mode 2 Error: Predefined site list missing for user {update.effective_user.id}")
        await safe_reply(chat_id, context, "⚠️ Error: Internal data lost (Sites). Please /start again.")
        return ConversationHandler.END

    num_sites = len(target_sites)
    limit_display = "all" if limit_value == 0 else f"{limit_value:,}"
    processing_text = f"⚙️ Processing: Filtering for {num_sites} common domains ({limit_display} results)..."
    processing_msg = await safe_reply(chat_id, context, processing_text)

    results = []
    try:
         results = await asyncio.to_thread(
             process_combo_filter_common, target_sites, limit_value if limit_value > 0 else None
         )
         logger.info(f"Mode 2: Found {len(results)} results for {num_sites} sites (limit: {limit_display})")
    except Exception as e:
         logger.error(f"Error filtering (Mode 2): {e}", exc_info=True)
         await safe_reply(chat_id, context, f"⚠️ Error during filtering: {str(e)}")
         await send_results_as_file(chat_id, context, [], f"M2_Filter_Common", "Error")
         return ConversationHandler.END
    finally:
        if processing_msg: await _delete_message_safely(processing_msg)

    await send_results_as_file(chat_id, context, results,
                               f"M2_Filter_Common",
                               f"📊 Mode 2 Results for {num_sites} Common Domains")
    return ConversationHandler.END

# --- Mode 3 Handlers ---
async def handle_search_term_m3(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles search term, asks for limit."""
    message = update.message
    search_term = message.text.strip() if message and message.text else ""
    chat_id = update.effective_chat.id
    prompt_message = context.user_data.get('active_prompt_message')

    await _delete_message_safely(message)

    if not search_term:
        error_text = "⚠️ Search term cannot be empty. Please enter text to search for:"
        if prompt_message:
            cancel_button = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data='cancel')]])
            full_error_prompt = f"🔍 **Mode 3: Search All Data**\n\n*{error_text}*\nPlease try again:"
            await safe_edit_message(prompt_message, context, full_error_prompt, reply_markup=cancel_button)
        else:
             await safe_reply(chat_id, context, error_text + "\nPlease try again:")
        return AWAITING_SEARCH_TERM_M3

    context.user_data['search_term'] = search_term
    logger.info(f"Mode 3: User {update.effective_user.id} entered search term: '{search_term}'")

    await _delete_message_safely(context.user_data.pop('active_prompt_message', None))

    limit_prompt_text = f"✅ Searching for: `{search_term}`\n🔢 Enter the maximum results (number or `all`):"
    cancel_button = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data='cancel')]])
    await safe_reply(chat_id, context, limit_prompt_text, reply_markup=cancel_button, store_message=True)

    return AWAITING_LIMIT_M3

async def handle_limit_m3(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles limit input for Mode 3. Processes in thread."""
    limit_prompt_message = context.user_data.get('active_prompt_message')
    chat_id = update.effective_chat.id

    limit_value = await parse_limit_input_and_edit_on_error(update, context, limit_prompt_message)
    if limit_value is None: return AWAITING_LIMIT_M3

    await _delete_message_safely(update.message)
    await _delete_message_safely(limit_prompt_message)
    context.user_data.pop('active_prompt_message', None)

    search_term = context.user_data.get('search_term')
    if not search_term:
        logger.error(f"Mode 3 Error: Search term missing for user {update.effective_user.id}")
        await safe_reply(chat_id, context, "⚠️ Error: Internal data lost (Search Term). Please /start again.")
        return ConversationHandler.END

    limit_display = "all" if limit_value == 0 else f"{limit_value:,}"
    processing_text = f"⚙️ Processing: Searching for `{search_term}` ({limit_display} results)..."
    processing_msg = await safe_reply(chat_id, context, processing_text)

    results = []
    try:
         results = await asyncio.to_thread(
             process_combo_search, search_term, limit_value if limit_value > 0 else None
         )
         logger.info(f"Mode 3: Found {len(results)} results for search '{search_term}' (limit: {limit_display})")
    except Exception as e:
         safe_term_fn = re.sub(r'[^\w\-]+', '_', search_term)[:20]
         logger.error(f"Error searching (Mode 3, term: '{search_term}'): {e}", exc_info=True)
         await safe_reply(chat_id, context, f"⚠️ Error during search: {str(e)}")
         await send_results_as_file(chat_id, context, [], f"M3_Search_{safe_term_fn}", "Error")
         return ConversationHandler.END
    finally:
         if processing_msg: await _delete_message_safely(processing_msg)

    safe_term_fn = re.sub(r'[^\w\-]+', '_', search_term)[:20]
    await send_results_as_file(chat_id, context, results,
                               f"M3_Search_{safe_term_fn}",
                               f"📊 Mode 3 Results for '{search_term}'")
    return ConversationHandler.END


# --- Utility & Fallback Handlers ---
async def parse_limit_input_and_edit_on_error(update: Update, context: ContextTypes.DEFAULT_TYPE, prompt_msg: Message | None) -> int | None:
    """Parses limit. Returns int >= 0 (0 for 'all'), or None for invalid. Edits prompt message on error."""
    if not update.message or not update.message.text: return None

    user_input = update.message.text.strip().lower()
    chat_id = update.effective_chat.id
    parsed_limit: int | None = None
    error_text: str | None = None
    base_prompt_text = "🔢 Enter the maximum results (number or `all`):" # Default base

    if prompt_msg and prompt_msg.text:
         original_lines = prompt_msg.text.split('\n')
         request_line_index = -1
         for i, line in enumerate(reversed(original_lines)):
             if "number or `all`" in line or "number or all" in line.lower():
                 request_line_index = len(original_lines) - 1 - i
                 break
         if request_line_index != -1:
             base_prompt_text = "\n".join(original_lines[:request_line_index]).strip()
         else:
             base_prompt_text = prompt_msg.text

    if user_input == 'all':
        parsed_limit = 0
    else:
        try:
            limit = int(user_input)
            if limit <= 0:
                error_text = "⚠️ Limit must be a *positive* number (e.g., `100`)."
            else:
                parsed_limit = limit
        except ValueError:
            error_text = "⚠️ Invalid input. Please enter a *number* (e.g., `50`) or the word `all`."

    if error_text:
        await _delete_message_safely(update.message)
        if prompt_msg:
            full_error_prompt = f"{base_prompt_text}\n\n*{error_text}*\nPlease try again:"
            cancel_button = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data='cancel')]])
            await safe_edit_message(prompt_msg, context, full_error_prompt, reply_markup=cancel_button)
        else:
            await safe_reply(chat_id, context, error_text + "\nPlease try again:")
        return None

    return parsed_limit

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the current operation, cleans up, and shows the main menu."""
    user = update.effective_user
    query = update.callback_query
    chat_id = update.effective_chat.id
    if not chat_id:
         logger.warning("Cancel handler couldn't determine chat_id.")
         return ConversationHandler.END # Cannot proceed

    log_msg = f"User {user.id if user else 'Unknown'} cancelled operation"
    text_to_send = '❌ Operation cancelled.'

    # --- Clean up state ---
    active_prompt_msg = context.user_data.pop('active_prompt_message', None)
    preserved_data = {
        'chat_id': chat_id,
        'has_seen_intro': context.user_data.get('has_seen_intro') # Keep intro status
    }
    context.user_data.clear()
    context.user_data.update(preserved_data)
    await _delete_message_safely(active_prompt_msg)
    # --- End cleanup ---

    target_message_to_delete = None
    if query:
        await query.answer()
        target_message_to_delete = query.message # Button's message
        log_msg += " via button."
    elif update.message:
        log_msg += " via /cancel command."
        await _delete_message_safely(update.message) # Delete the /cancel command
    else:
         log_msg += " (unknown source)."

    logger.info(log_msg)
    await _delete_message_safely(target_message_to_delete) # Delete button message if applicable

    # Send confirmation and show main menu again
    await safe_reply(chat_id, context, text_to_send)
    # Retrieve user name for menu display
    user_first_name = preserved_data.get('first_name') or (user.first_name if user else None) or "User"
    await show_main_menu(chat_id, context, user_first_name)

    # Return to the mode selection state within the conversation
    return SELECTING_MODE


async def handle_unexpected_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles unexpected text messages within the conversation."""
    if not update.message or not update.message.text: return
    chat_id = update.effective_chat.id
    logger.warning(f"User {update.effective_user.id} sent unexpected text: '{update.message.text}' during conversation.")
    await _delete_message_safely(update.message)
    await safe_reply(chat_id, context, "🤔 Unexpected input. Please respond to the prompt above or use /cancel to stop.")

async def handle_unexpected_attachment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles unexpected attachments during conversation."""
    if not update.message or not update.message.effective_attachment: return
    chat_id = update.effective_chat.id
    logger.warning(f"User {update.effective_user.id} sent unexpected attachment during conversation.")
    await _delete_message_safely(update.message)
    await safe_reply(chat_id, context, "🤔 Attachments are not expected here. Please respond to the prompt above or use /cancel to stop.")

async def handle_general_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles any text message outside the conversation flow."""
    if update.message and update.message.text and not update.message.text.startswith('/'):
         logger.info(f"User {update.effective_user.id} sent general text: '{update.message.text}'")
         await _delete_message_safely(update.message)
         await safe_reply(update.effective_chat.id, context, "👋 Hi there! Please use /start to see the options.")

# --- Bot Initialization & Error Handling ---
async def post_init(application: Application) -> None:
    """Determine paths, load combo file, and log bot readiness."""
    global SCRIPT_DIR, FULL_COMBO_PATH
    try:
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    except NameError:
         SCRIPT_DIR = os.getcwd()
         logger.warning(f"__file__ not defined, using CWD: {SCRIPT_DIR}")

    FULL_COMBO_PATH = os.path.join(SCRIPT_DIR, DEFAULT_COMBO_FILE)
    logger.info(f"Script directory: {SCRIPT_DIR}")
    logger.info(f"Combo file path: {FULL_COMBO_PATH}")
    load_combo_file()

    bot_info = await application.bot.get_me()
    logger.info(f"Bot {bot_info.first_name} (ID: {bot_info.id}) online and ready!")
    if not COMBO_LOAD_SUCCESS and DEFAULT_COMBO_FILE:
         logger.critical(f"!!! DATA FILE '{DEFAULT_COMBO_FILE}' FAILED TO LOAD. MODES 1-3 UNAVAILABLE !!!")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log Errors and notify user if appropriate."""
    logger.error(f"Exception handling update: {context.error}", exc_info=context.error)

    chat_id: int | None = None
    user_id: int | str = "N/A"
    if isinstance(update, Update):
        if update.effective_chat: chat_id = update.effective_chat.id
        if update.effective_user: user_id = update.effective_user.id
    elif context.chat_data and 'chat_id' in context.chat_data:
        chat_id = context.chat_data['chat_id']

    ignore_errors = (telegram.error.Forbidden, telegram.error.ChatMigrated, telegram.error.NetworkError)
    if isinstance(context.error, ignore_errors):
         logger.warning(f"Ignoring expected error: {context.error}")
         return
    if isinstance(context.error, telegram.error.BadRequest):
         error_str = str(context.error).lower()
         if "message is not modified" in error_str:
             logger.info("Ignoring 'Message is not modified'.")
             return
         if "message to edit not found" in error_str:
             logger.warning("Ignoring 'message to edit not found'.")
             return
         if "message to delete not found" in error_str:
             logger.warning("Ignoring 'message to delete not found'.")
             return
         if "chat not found" in error_str:
              logger.warning(f"Ignoring 'Chat not found' for chat_id {chat_id}")
              return
         if "bot was blocked by the user" in error_str:
              logger.warning(f"Bot blocked by user {user_id} in chat {chat_id}")
              return

    if chat_id:
        try:
            error_text = "⚠️ An unexpected internal error occurred. Please try /start again."
            await safe_reply(chat_id, context, error_text)
        except Exception as e_reply:
            logger.error(f"Failed sending error notification to user {user_id} in chat {chat_id}: {e_reply}")

# --- Main Function ---
def main() -> None:
    """Configures and runs the bot application."""
    logger.info("--- Bot Starting ---")

    application = ApplicationBuilder()\
        .token(TELEGRAM_BOT_TOKEN)\
        .post_init(post_init)\
        .connect_timeout(30)\
        .read_timeout(30)\
        .build()

    # Filters
    private_chat_filter = filters.ChatType.PRIVATE
    private_text = filters.TEXT & ~filters.COMMAND & private_chat_filter
    private_attachment = filters.ATTACHMENT & private_chat_filter

    # --- Conversation Handler Setup ---
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start, private_chat_filter)],
        states={
            SELECTING_MODE: [CallbackQueryHandler(select_mode, pattern='^mode_[1-3]$')],
            # Mode 1
            AWAITING_SINGLE_SITE: [MessageHandler(private_text, handle_site_input_m1)],
            AWAITING_LIMIT_M1: [MessageHandler(private_text, handle_limit_m1)],
            # Mode 2
            AWAITING_LIMIT_M2: [MessageHandler(private_text, handle_limit_m2)],
            # Mode 3
            AWAITING_SEARCH_TERM_M3: [MessageHandler(private_text, handle_search_term_m3)],
            AWAITING_LIMIT_M3: [MessageHandler(private_text, handle_limit_m3)],
        },
        fallbacks=[
            CallbackQueryHandler(cancel, pattern='^cancel$'), # Returns SELECTING_MODE
            CommandHandler('cancel', cancel, private_chat_filter), # Returns SELECTING_MODE
            # Unexpected input handlers within conversation
            MessageHandler(private_text, handle_unexpected_message),
            MessageHandler(private_attachment, handle_unexpected_attachment),
            # Start command can interrupt and restart the conversation cleanly
            CommandHandler('start', start, private_chat_filter),
        ],
        name="main_conversation",
        allow_reentry=True,
    )

    # --- Add handlers ---
    application.add_handler(conv_handler)
    # Handler for 'Start Again' button (outside conversation - calls start_again which calls start)
    application.add_handler(CallbackQueryHandler(start_again, pattern='^start_again$'))
    # Handler for general text outside conversation
    application.add_handler(MessageHandler(private_text, handle_general_text))
    # Error handler (last)
    application.add_error_handler(error_handler)

    # --- Run the bot ---
    logger.info("Starting bot polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    logger.info("--- Bot Stopped ---")

# --- Script Entry Point ---
if __name__ == '__main__':
    main()
