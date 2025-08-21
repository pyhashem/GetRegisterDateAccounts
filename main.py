import asyncio
from pathlib import Path
import shutil
from loguru import logger
from pyutilities import SESSION, PROXY
from pyutilities.singlefunc import chunk_generate

from telethon import TelegramClient, events, functions


logger = logger.opt(colors=True)
SELF_PATH: Path = Path('self/self.session')
SESSIONS_PATH: Path = Path('sessions')
CHUNK_SIZE: int = 100
TRIGGER: str = '👋🏻'

shared_data: dict = dict(
    self_user_id=None,
    self_username=None, # TODO if self not have username set it new to can send message 
)

creation_dates: dict[int, dict] = {}


async def worker_transfer_files():
    """ Worker to transfer session files based on registration dates."""
    # logger.debug(creation_dates)
    # logger.info("Starting worker to transfer files...")

    while True: 
        keys_to_remove = [k for k, v in creation_dates.items() if v.get("moved") is True]
        for k in keys_to_remove:
            del creation_dates[k]

        try:
            for user_id, data in creation_dates.items():
                is_moved = data.get('moved', False)

                if is_moved:
                    continue

                registration_date = data.get('date')
                session_path = Path(data.get('path')) if data.get('path') else None

                if registration_date == None:
                    continue
                
                if not session_path:
                    logger.warning(f"No session path found for user {user_id}. Skipping...")
                    continue

                if session_path.exists():
                    logger.info(f"Transferring session file for user {user_id}...")

                    destination_path = session_path.parent.joinpath(str(registration_date))
                    destination_path.mkdir(parents=True, exist_ok=True)

                    await asyncio.to_thread(shutil.move, session_path, destination_path.joinpath(session_path.name))
                    logger.info(f"Session file for user {user_id} moved to {destination_path}")

                    session_json_path = session_path.with_suffix('.json')
                    if session_json_path.exists():
                        logger.info(f"Moving JSON file for user {user_id} to {destination_path}")
                        await asyncio.to_thread(shutil.move, session_json_path, destination_path.joinpath(session_json_path.name))
                    
                    
                    creation_dates[user_id]['moved'] = True

        except Exception as e:
            logger.exception(f"Error in worker_transfer_files: {e}")
            continue

        await asyncio.sleep(1)  # Wait for N seconds before the next iteration
        break  # Remove this line to keep the worker running indefinitely


@events.register(events.NewMessage(pattern=TRIGGER, func=lambda e: e.is_private))
async def handle_new_message(event):
    logger.info(f"New message received from user <g>{event.sender_id}</g>.")
    # await event.respond(TRIGGER)
    await event.mark_read()
    # print(f"Event: {event.stringify()}")


    try:
        eninity = await event.client.get_entity(event.sender_id)
        result = await event.client(functions.messages.GetPeerSettingsRequest(
            peer=eninity
        ))
            

        # print(result.stringify())
        registration_month = result.settings.registration_month

        if not creation_dates.get(event.sender_id):
            creation_dates[event.sender_id] = {
                'date': registration_month,
                'path': None,
            }
        
        else:
            creation_dates[event.sender_id]['date'] = registration_month
        
        logger.info(f"Updated registration date for user <g>{event.sender_id} to {registration_month}</g>.")

    except Exception as e:
        logger.error(f"Error handling new message from user {event.sender_id}: {e}")
        return


async def on_self_startup(retrys: int = 5) -> TelegramClient:

    logger.info("Initiating self startup...")

    session = SESSION(SELF_PATH)
    client: TelegramClient = session.get_client(connection_retries=1, timeout=5)
    proxy = PROXY.getProxy()

    try:
        if not proxy:
            raise Exception("No proxy available for self startup.")
    
        client.set_proxy(proxy=proxy)

        await asyncio.wait_for(client.connect(), timeout=10)
        me = await client.get_me()
        if me is None:
            logger.error(f"{session.session_path.name} Account is None, skipping...")
            return None
        
        shared_data['self_user_id'] = me.id
        shared_data['self_username'] = me.username if me.username else None

        logger.info(f"Self startup successful. Logged in as: <m>{me.first_name} ({me.id})</m>")
        
        return client
    
    except (ConnectionError, asyncio.TimeoutError, asyncio.IncompleteReadError, ValueError, OSError):
        logger.error("Failed to connect to Telegram with the provided proxy.")
        if retrys > 0:
            logger.info(f"Retrying self startup... ({retrys} attempts left)")
            return await on_self_startup(retrys - 1)
    
        else:
            logger.error("Max retries reached. Self startup failed.")
            raise Exception("Self startup failed after multiple attempts.")

    except Exception as e:
        logger.error(f"Error during self startup: {e}")
        return


async def session_client(session_path: Path, retrys: int = 5) -> None:
    """Create a Telegram client for a specific session."""
    session = SESSION(session_path)
    client: TelegramClient = session.get_client(connection_retries=1, timeout=5)
    proxy = PROXY.getProxy()

    try:
        if not proxy:
            raise Exception(f"{session.session_path.name} | No proxy available for session.")
    
        client.set_proxy(proxy=proxy)

        await asyncio.wait_for(client.connect(), timeout=10)

        me = await client.get_me()
        if me is None:
            logger.error(f"{session.session_path.name} Account is None, skipping...")
            await client.disconnect()
            await asyncio.sleep(1)  # Wait for client to disconnect properly

            if session_path.exists():
                destination_path = session_path.parent.joinpath(str('deleted'))
                destination_path.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(shutil.move, session_path, destination_path.joinpath(session_path.name))
                
                session_json_path = session_path.with_suffix('.json')
                if session_json_path.exists():
                    await asyncio.to_thread(shutil.move, session_json_path, destination_path.joinpath(session_json_path.name))
            
            await asyncio.sleep(1)  # Wait for client to disconnect properly
            return None
        
        if not creation_dates.get(me.id):
            creation_dates[me.id] = {
                'date': None,
                'path': session_path.as_posix(),
            }
        
        username: str | None = shared_data.get('self_username')
        if not username:
            logger.error(f"{session.session_path.name} | No username found for self account, skipping...")
            await client.disconnect()
            return None
        

        await client.send_message(username, TRIGGER)
        logger.info('Message sent to self account to trigger registration date update.')
        await asyncio.sleep(1)  # Wait for the message to be processed
        await client.disconnect()

        await asyncio.sleep(2)  # Wait for the client to disconnect properly

        return
    
    except (ConnectionError, asyncio.TimeoutError, asyncio.IncompleteReadError, ValueError) as e:
        logger.error(f"Failed to connect with session {session_path.name}: {e}")
        if retrys > 0:
            logger.info(f"Retrying session client for {session_path.name}... ({retrys} attempts left)")
            await client.disconnect()
            return await session_client(session_path, retrys - 1)
    
        else:
            logger.error(f"Max retries reached for session {session_path.name}.")
            await client.disconnect()
            raise Exception(f"Session client failed after multiple attempts: {session_path.name}")
    
    except Exception as e:
        logger.error(f"Error during session client creation for {session_path.name}: {e}")
        await client.disconnect()
        return None


async def sessions_worker():
    """Worker to handle session-related tasks."""
    logger.info("Starting sessions worker...")
    sessions: list[Path] = [x for x in SESSIONS_PATH.iterdir() if x.is_file() and x.suffix == '.session']

    if not sessions:
        logger.warning("No session files found in the sessions directory.")
        return
    
    chunked_sessions = chunk_generate(sessions, chunk_size=CHUNK_SIZE)

    for chunk in chunked_sessions:
        logger.info(f"Processing chunk of {len(chunk)} sessions...")
        await asyncio.gather(*(session_client(session) for session in chunk))
        await asyncio.sleep(1)  # Wait for N seconds before processing the next chunk

        logger.info("Chunk processing complete. Waiting for file transfer worker to finish...")
        await worker_transfer_files()


async def main():
    client: TelegramClient = None

    try:
        client: TelegramClient = await on_self_startup()
    
    except Exception as e:
        logger.error(f"Failed to start self client: {e}")
        return
    
    if not client:
        logger.error("Client is None, exiting...")
        return
    
    client.add_event_handler(handle_new_message)

    # loop.create_task(worker_transfer_files())
    await sessions_worker()
    logger.info("<c>Sessions worker Done.</c>")


    await client.disconnect()
    logger.info("Self client disconnected.")

    # await client.run_until_disconnected()



if __name__ == "__main__":
    PROXY.load()
    logger.info("Starting the application...")
    logger.info(f"Total Using proxy: <c>{len(PROXY.proxys)}</c>")
    loop = asyncio.new_event_loop()

    try:
        if not SESSIONS_PATH.exists():
            logger.info(f"Creating sessions directory at {SESSIONS_PATH}")
            SESSIONS_PATH.mkdir(parents=True, exist_ok=True)
            raise Exception(f"Sessions directory is empty : {SESSIONS_PATH}")
        
        if not SELF_PATH.exists():
            raise Exception(f"Self session file does not exist in {SELF_PATH}")

        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())

    except KeyboardInterrupt:
        logger.info("Application stopped by user.")
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        loop.close()
