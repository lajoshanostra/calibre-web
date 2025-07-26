# -*- coding: utf-8 -*-

#  This file is part of the Calibre-Web (https://github.com/janeczku/calibre-web)
#    Copyright (C) 2012-2019 mutschler, jkrehm, cervinko, janeczku, OzzieIsaacs, csitko
#                            ok11, issmirnov, idalin
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program. If not, see <http://www.gnu.org/licenses/>.

import os
import re
from datetime import datetime, timezone
from . import logger, config, ub, db
from .gdriveutils import getFileFromEbooksFolder, uploadFileToEbooksFolder
import tempfile

log = logger.create()

def sanitize_filename(filename):
    """Sanitize filename for safe file operations."""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def get_moonreader_filename(book):
    """Generate Moonreader position filename from book metadata."""
    if hasattr(book, 'title') and book.title:
        title = sanitize_filename(book.title)
    else:
        title = f"book_{book.id}"
    
    return f"{title}.epub.po"

def kobo_to_moonreader_position(kobo_reading_state):
    """Convert Kobo reading state to Moonreader .epub.po format.
    
    Moonreader format: timestamp*chapter@session#position:percentage%
    Example: 1697743052498*11@0#1328:30.0%
    """
    if not kobo_reading_state or not kobo_reading_state.current_bookmark:
        return None
    
    bookmark = kobo_reading_state.current_bookmark
    
    # Use last_modified timestamp in milliseconds
    timestamp = int(kobo_reading_state.last_modified.timestamp() * 1000)
    
    # Extract chapter from location_value if available, default to 0
    chapter = 0
    if bookmark.location_value:
        try:
            chapter = int(re.search(r'\d+', str(bookmark.location_value)).group())
        except (AttributeError, ValueError):
            chapter = 0
    
    # Session ID (always 0 for now)
    session = 0
    
    # Character position from location_value or default
    position = 0
    if bookmark.location_value:
        try:
            position = int(bookmark.location_value.split('/')[-1]) if '/' in str(bookmark.location_value) else int(bookmark.location_value)
        except (ValueError, AttributeError):
            position = 0
    
    # Progress percentage
    progress = bookmark.progress_percent or 0.0
    
    return f"{timestamp}*{chapter}@{session}#{position}:{progress:.1f}%"

def moonreader_to_kobo_position(moonreader_content):
    """Parse Moonreader .epub.po format back to Kobo position data.
    
    Returns dict with timestamp, progress_percent, location_value
    """
    if not moonreader_content or not moonreader_content.strip():
        return None
    
    try:
        content = moonreader_content.strip()
        
        # Parse format: timestamp*chapter@session#position:percentage%
        parts = re.match(r'(\d+)\*(\d+)@(\d+)#(\d+):([0-9.]+)%', content)
        if not parts:
            log.warning(f"Invalid Moonreader position format: {content}")
            return None
        
        timestamp_ms, chapter, session, position, progress = parts.groups()
        
        return {
            'timestamp': datetime.fromtimestamp(int(timestamp_ms) / 1000, timezone.utc),
            'progress_percent': float(progress),
            'location_value': f"{chapter}/{position}",
            'location_type': 'text',
            'location_source': 'moonreader'
        }
    except Exception as e:
        log.error(f"Error parsing Moonreader position: {e}")
        return None

def create_position_file(book, kobo_reading_state, user_id):
    """Create and upload Moonreader position file to user's Google Drive."""
    if not kobo_reading_state or not kobo_reading_state.current_bookmark:
        log.debug("No reading position to sync")
        return False
    
    try:
        from . import moonreader_gdrive_utils
        
        # Get user's Google Drive connection
        user_gdrive = moonreader_gdrive_utils.UserGdriveAuth(user_id)
        if not user_gdrive.is_authenticated():
            log.debug(f"User {user_id} not authenticated with Google Drive, skipping Moonreader sync")
            return False
        
        position_content = kobo_to_moonreader_position(kobo_reading_state)
        if not position_content:
            log.warning("Failed to convert Kobo position to Moonreader format")
            return False
        
        filename = get_moonreader_filename(book)
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.po', delete=False) as temp_file:
            temp_file.write(position_content)
            temp_path = temp_file.name
        
        try:
            # Upload to user's Google Drive (use custom folder or default)
            folder_path = user_gdrive.credentials_record.folder_name or "/Apps/Books/.Moon+/Cache"
            success = user_gdrive.upload_file_to_folder(temp_path, filename, folder_path)
            
            if success:
                log.info(f"Uploaded Moonreader position file: {filename} for user {user_id}")
                return True
            else:
                log.error(f"Failed to upload Moonreader position file: {filename} for user {user_id}")
                return False
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_path)
            except OSError:
                pass
        
    except Exception as e:
        log.error(f"Error creating Moonreader position file for user {user_id}: {e}")
        return False

def check_moonreader_position_updates(user_id, book_id):
    """Check user's Google Drive for updated Moonreader position files.
    
    Returns updated position data if Moonreader position is newer than Kobo position.
    """
    try:
        from . import moonreader_gdrive_utils
        
        # Get user's Google Drive connection
        user_gdrive = moonreader_gdrive_utils.UserGdriveAuth(user_id)
        if not user_gdrive.is_authenticated():
            log.debug(f"User {user_id} not authenticated with Google Drive, skipping position check")
            return None
        
        # Get book info
        book = db.session.query(db.Books).filter(db.Books.id == book_id).first()
        if not book:
            return None
        
        # Get current Kobo reading state
        kobo_state = ub.session.query(ub.KoboReadingState).filter(
            ub.KoboReadingState.user_id == user_id,
            ub.KoboReadingState.book_id == book_id
        ).first()
        
        filename = get_moonreader_filename(book)
        folder_path = user_gdrive.credentials_record.folder_name or "/Apps/Books/.Moon+/Cache"
        
        # Download position file from user's Google Drive
        position_file = user_gdrive.download_file_from_folder(filename, folder_path)
        if not position_file:
            log.debug(f"No Moonreader position file found: {filename} for user {user_id}")
            return None
        
        # Read file content
        position_content = position_file.GetContentString()
        moonreader_data = moonreader_to_kobo_position(position_content)
        
        if not moonreader_data:
            log.warning(f"Invalid Moonreader position data in {filename} for user {user_id}")
            return None
        
        # Check if Moonreader position is newer than Kobo position
        if kobo_state and kobo_state.last_modified:
            if moonreader_data['timestamp'] <= kobo_state.last_modified:
                log.debug(f"Moonreader position not newer than Kobo position for {filename}")
                return None
        
        log.info(f"Found newer Moonreader position for {filename} for user {user_id}")
        return moonreader_data
        
    except Exception as e:
        log.error(f"Error checking Moonreader position updates for user {user_id}: {e}")
        return None

def update_kobo_reading_state_from_moonreader(user_id, book_id, moonreader_data):
    """Update Kobo reading state with Moonreader position data."""
    try:
        # Get or create Kobo reading state
        kobo_state = ub.session.query(ub.KoboReadingState).filter(
            ub.KoboReadingState.user_id == user_id,
            ub.KoboReadingState.book_id == book_id
        ).first()
        
        if not kobo_state:
            kobo_state = ub.KoboReadingState(user_id=user_id, book_id=book_id)
            ub.session.add(kobo_state)
            ub.session.flush()  # Get the ID
        
        # Get or create bookmark
        if not kobo_state.current_bookmark:
            kobo_state.current_bookmark = ub.KoboBookmark(kobo_reading_state_id=kobo_state.id)
            ub.session.add(kobo_state.current_bookmark)
        
        # Update bookmark with Moonreader data
        bookmark = kobo_state.current_bookmark
        bookmark.progress_percent = moonreader_data['progress_percent']
        bookmark.location_value = moonreader_data['location_value']
        bookmark.location_type = moonreader_data['location_type']
        bookmark.location_source = moonreader_data['location_source']
        bookmark.last_modified = moonreader_data['timestamp']
        
        # Update reading state timestamp
        kobo_state.last_modified = moonreader_data['timestamp']
        kobo_state.priority_timestamp = moonreader_data['timestamp']
        
        ub.session.commit()
        log.info(f"Updated Kobo reading state from Moonreader for book {book_id}")
        return True
        
    except Exception as e:
        log.error(f"Error updating Kobo reading state from Moonreader: {e}")
        ub.session.rollback()
        return False