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

def get_book_format(book):
    """Detect if book is .kepub or .epub format."""
    if hasattr(book, 'data'):
        formats = [data.format.upper() for data in book.data]
        if 'KEPUB' in formats:
            return 'kepub'
        elif 'EPUB' in formats:
            return 'epub'
    return 'epub'  # Default to epub if uncertain

def find_existing_moonreader_file(book, user_gdrive, folder_path):
    """Find existing MoonReader position file in user's Google Drive folder.
    
    Tries multiple naming patterns to match files created by MoonReader vs Calibre-Web.
    Returns the filename of existing file, or None if not found.
    """
    try:
        # Get list of all .epub.po files in the folder
        existing_files = user_gdrive.list_files_in_folder(folder_path, "*.epub.po")
        if not existing_files:
            return None
        
        # Extract just the filenames for comparison
        existing_filenames = [f['title'] for f in existing_files]
        
        # Prepare search terms
        book_title = book.title if hasattr(book, 'title') and book.title else f"book_{book.id}"
        sanitized_title = sanitize_filename(book_title)
        
        # 1. Try exact match with current Calibre-Web naming
        exact_name = f"{sanitized_title}.epub.po"
        if exact_name in existing_filenames:
            log.debug(f"Found exact filename match: {exact_name}")
            return exact_name
        
        # 2. Try with author (common MoonReader pattern)
        if hasattr(book, 'authors') and book.authors:
            author_name = book.authors[0].name
            with_author = f"{sanitized_title} - {sanitize_filename(author_name)}.epub.po"
            if with_author in existing_filenames:
                log.debug(f"Found filename with author: {with_author}")
                return with_author
        
        # 3. Try fuzzy matching - look for files containing the book title
        sanitized_lower = sanitized_title.lower()
        for filename in existing_filenames:
            filename_lower = filename.lower()
            # Check if the book title is contained in the filename
            if sanitized_lower in filename_lower and filename_lower.endswith('.epub.po'):
                log.debug(f"Found fuzzy match: {filename} (contains '{sanitized_title}')")
                return filename
        
        # 4. Try reverse fuzzy matching - check if filename title is in book title
        for filename in existing_filenames:
            if filename.endswith('.epub.po'):
                # Extract title part (remove .epub.po and potential author)
                file_title = filename[:-8]  # Remove .epub.po
                if ' - ' in file_title:
                    file_title = file_title.split(' - ')[0]  # Remove author part
                
                file_title_lower = file_title.lower()
                if file_title_lower and file_title_lower in sanitized_lower:
                    log.debug(f"Found reverse fuzzy match: {filename} ('{file_title}' in '{sanitized_title}')")
                    return filename
        
        log.debug(f"No existing MoonReader file found for book '{book_title}'")
        return None
        
    except Exception as e:
        log.error(f"Error searching for existing MoonReader file: {e}")
        return None

def get_moonreader_filename(book, user_gdrive=None, folder_path=None):
    """Generate Moonreader position filename from book metadata.
    
    If user_gdrive and folder_path are provided, will search for existing files first.
    """
    # Try to find existing file first
    if user_gdrive and folder_path:
        existing_filename = find_existing_moonreader_file(book, user_gdrive, folder_path)
        if existing_filename:
            return existing_filename
    
    # Generate new filename if no existing file found
    if hasattr(book, 'title') and book.title:
        title = sanitize_filename(book.title)
    else:
        title = f"book_{book.id}"
    
    return f"{title}.epub.po"

def kobo_to_moonreader_position(kobo_reading_state, book=None):
    """Convert Kobo reading state to Moonreader .epub.po format.
    
    Moonreader format: timestamp*chapter@offset#line:percentage%
    - timestamp: Unix timestamp in milliseconds
    - chapter: Zero-based chapter index
    - offset: Paragraph/line offset within chapter
    - line: Screen/page offset
    - percentage: Progress through entire book
    
    Example: 1697743052498*2@0#0:1.7%
    """
    if not kobo_reading_state or not kobo_reading_state.current_bookmark:
        return None
    
    bookmark = kobo_reading_state.current_bookmark
    
    # Use last_modified timestamp in milliseconds
    timestamp = int(kobo_reading_state.last_modified.timestamp() * 1000)
    
    # Detect book format for proper location parsing
    book_format = get_book_format(book) if book else 'epub'
    
    # Extract chapter and position based on format
    chapter = 0
    paragraph_offset = 0
    line_offset = 0
    
    if bookmark.location_value:
        if book_format == 'kepub' and '!' in str(bookmark.location_value):
            # Parse .kepub ContentID format: book.kepub.epub!OEBPS/Text/Chapter2.xhtml#kobo.2.1234
            try:
                location_str = str(bookmark.location_value)
                log.debug(f"Parsing kepub location: {location_str}")
                
                # Extract chapter from path like "Chapter2.xhtml" or "chapter2.html"
                chapter_match = re.search(r'[Cc]hapter(\d+)', location_str)
                if chapter_match:
                    # Convert to zero-based indexing for MoonReader
                    chapter = max(0, int(chapter_match.group(1)) - 1)
                else:
                    # Try to extract from numbered files like "002.xhtml" 
                    file_match = re.search(r'/(\d+)\.x?html', location_str)
                    if file_match:
                        # Already zero-based if using file numbering
                        chapter = int(file_match.group(1))
                
                # Extract position from kobo fragment like "#kobo.2.1234"
                kobo_match = re.search(r'#kobo\.(\d+)\.(\d+)', location_str)
                if kobo_match:
                    kobo_chapter = int(kobo_match.group(1))
                    kobo_position = int(kobo_match.group(2))
                    
                    # Use kobo position to estimate paragraph and line offsets
                    # This is an approximation - MoonReader's actual calculation is proprietary
                    paragraph_offset = kobo_position // 100  # Rough estimate: 100 chars per paragraph
                    line_offset = kobo_position % 100  # Remainder as line offset within paragraph
                    
                log.debug(f"Kepub parsed - chapter: {chapter}, paragraph_offset: {paragraph_offset}, line_offset: {line_offset}")
                    
            except (ValueError, AttributeError) as e:
                log.debug(f"Failed to parse .kepub location_value '{bookmark.location_value}': {e}")
                chapter = 0
                paragraph_offset = 0
                line_offset = 0
        else:
            # Parse .epub format or fallback - simpler chapter/position format
            try:
                if '/' in str(bookmark.location_value):
                    # Format like "2/1234" 
                    parts = str(bookmark.location_value).split('/')
                    if parts[0].isdigit():
                        # Convert to zero-based indexing for MoonReader
                        chapter = max(0, int(parts[0]) - 1)
                    else:
                        chapter = 0
                    
                    if len(parts) > 1 and parts[1].isdigit():
                        position = int(parts[1])
                        # Estimate paragraph and line offsets from position
                        paragraph_offset = position // 50  # Rough estimate for epub
                        line_offset = position % 50
                    else:
                        paragraph_offset = 0
                        line_offset = 0
                else:
                    # Single number - extract and estimate offsets
                    position_match = re.search(r'\d+', str(bookmark.location_value))
                    if position_match:
                        position = int(position_match.group())
                        chapter = 0  # Default to first chapter
                        paragraph_offset = position // 50
                        line_offset = position % 50
                    else:
                        chapter = 0
                        paragraph_offset = 0
                        line_offset = 0
                        
                log.debug(f"Epub parsed - chapter: {chapter}, paragraph_offset: {paragraph_offset}, line_offset: {line_offset}")
                        
            except (ValueError, AttributeError) as e:
                log.debug(f"Failed to parse epub location_value '{bookmark.location_value}': {e}")
                chapter = 0
                paragraph_offset = 0
                line_offset = 0
    
    # Progress percentage
    progress = bookmark.progress_percent or 0.0
    
    log.debug(f"Final MoonReader position: {timestamp}*{chapter}@{paragraph_offset}#{line_offset}:{progress:.1f}%")
    return f"{timestamp}*{chapter}@{paragraph_offset}#{line_offset}:{progress:.1f}%"

def moonreader_to_kobo_position(moonreader_content, book=None):
    """Parse Moonreader .epub.po format back to Kobo position data.
    
    Moonreader format: timestamp*chapter@offset#line:percentage%
    - chapter: Zero-based chapter index (convert to 1-based for Kobo)
    - offset: Paragraph/line offset within chapter
    - line: Screen/page offset
    
    Returns dict with timestamp, progress_percent, location_value
    """
    if not moonreader_content or not moonreader_content.strip():
        return None
    
    try:
        content = moonreader_content.strip()
        log.debug(f"Parsing MoonReader content: {content}")
        
        # Parse format: timestamp*chapter@offset#line:percentage%
        parts = re.match(r'(\d+)\*(\d+)@(\d+)#(\d+):([0-9.]+)%', content)
        if not parts:
            log.warning(f"Invalid Moonreader position format: {content}")
            return None
        
        timestamp_ms, chapter, paragraph_offset, line_offset, progress = parts.groups()
        
        # Convert zero-based chapter to 1-based for Kobo
        kobo_chapter = int(chapter) + 1
        
        # Reconstruct approximate position from paragraph and line offsets
        # This is an approximation since we don't have the exact conversion
        estimated_position = int(paragraph_offset) * 100 + int(line_offset)
        
        log.debug(f"Parsed - chapter: {chapter} (MR) -> {kobo_chapter} (Kobo), estimated_position: {estimated_position}")
        
        # Detect book format for proper location_value generation
        book_format = get_book_format(book) if book else 'epub'
        
        if book_format == 'kepub':
            # Generate .kepub ContentID format: book.kepub.epub!OEBPS/Text/ChapterX.xhtml#kobo.Y.Z
            book_filename = sanitize_filename(book.title) if book and hasattr(book, 'title') else 'book'
            
            # Generate ContentID with chapter and position
            location_value = f"{book_filename}.kepub.epub!OEBPS/Text/Chapter{kobo_chapter}.xhtml#kobo.{kobo_chapter}.{estimated_position}"
            location_type = 'kobo'
            location_source = 'calibre-web'
        else:
            # Use simple .epub format: chapter/position
            location_value = f"{kobo_chapter}/{estimated_position}"
            location_type = 'text'
            location_source = 'moonreader'
        
        log.debug(f"Generated Kobo location_value: {location_value}")
        
        return {
            'timestamp': datetime.fromtimestamp(int(timestamp_ms) / 1000, timezone.utc),
            'progress_percent': float(progress),
            'location_value': location_value,
            'location_type': location_type,
            'location_source': location_source
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
        
        position_content = kobo_to_moonreader_position(kobo_reading_state, book)
        if not position_content:
            log.warning("Failed to convert Kobo position to Moonreader format")
            return False
        
        # Get sync folder path
        folder_path = user_gdrive.credentials_record.folder_name or "/Apps/Books/.Moon+/Cache"
        
        # Use smart filename matching to find existing files or generate new name
        filename = get_moonreader_filename(book, user_gdrive, folder_path)
        
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
        
        # Get sync folder path
        folder_path = user_gdrive.credentials_record.folder_name or "/Apps/Books/.Moon+/Cache"
        
        # Use smart filename matching to find existing files
        filename = get_moonreader_filename(book, user_gdrive, folder_path)
        
        # Download position file from user's Google Drive
        position_file = user_gdrive.download_file_from_folder(filename, folder_path)
        if not position_file:
            log.debug(f"No Moonreader position file found: {filename} for user {user_id}")
            return None
        
        # Read file content
        position_content = position_file.GetContentString()
        moonreader_data = moonreader_to_kobo_position(position_content, book)
        
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