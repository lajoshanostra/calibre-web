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

def get_epub_content_length(book):
    """Get approximate character count for an epub book."""
    try:
        # This would need to be implemented to analyze the actual EPUB content
        # For now, we'll use a rough estimate based on file size or page count
        if hasattr(book, 'data'):
            for data in book.data:
                if data.format.upper() in ['EPUB', 'KEPUB']:
                    # Rough estimate: assume 2000 chars per "page"
                    # This should be replaced with actual content analysis
                    return 200000  # Default estimate
        return 200000
    except:
        return 200000

def kobo_to_moonreader_position(kobo_reading_state, book=None):
    """Convert Kobo reading state to Moonreader .epub.po format.
    
    Moonreader format: timestamp*chapter@scrolled_chars#screen_offset:percentage%
    - timestamp: Unix timestamp in milliseconds
    - chapter: Zero-based chapter index
    - scrolled_chars: Characters scrolled past in current view (often 0 at chapter start)
    - screen_offset: Position within current screen/page
    - percentage: Progress through CURRENT CHAPTER (not entire book!)
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
    scrolled_chars = 0  # This should usually be 0 unless we're deep into a chapter
    screen_offset = 0
    
    # Add detailed logging for debugging
    log.info(f"Converting Kobo position - book_format: {book_format}, location_value: '{bookmark.location_value}', progress: {bookmark.progress_percent}%")
    
    if bookmark.location_value:
        if book_format == 'kepub' and '!' in str(bookmark.location_value):
            # Parse .kepub ContentID format
            try:
                location_str = str(bookmark.location_value)
                log.debug(f"Parsing kepub location: {location_str}")
                
                # Extract chapter from path - try multiple patterns
                chapter_found = False
                
                # Pattern 1: Chapter1.xhtml, chapter1.html, etc.
                chapter_match = re.search(r'[Cc]hapter[-_]?(\d+)', location_str)
                if chapter_match:
                    chapter = max(0, int(chapter_match.group(1)) - 1)
                    chapter_found = True
                    log.debug(f"Found chapter via Chapter pattern: {chapter}")
                
                # Pattern 2: Numbered files like 001.xhtml, part2.html, etc.
                if not chapter_found:
                    file_match = re.search(r'/(?:part|section|ch)?[-_]?(\d+)\.x?html?', location_str, re.IGNORECASE)
                    if file_match:
                        chapter = max(0, int(file_match.group(1)) - 1)
                        chapter_found = True
                        log.debug(f"Found chapter via numbered file pattern: {chapter}")
                
                # Pattern 3: Extract from full path structure
                if not chapter_found:
                    path_match = re.search(r'/(?:text|content|html)/[^/]*?(\d+)', location_str, re.IGNORECASE)
                    if path_match:
                        chapter = max(0, int(path_match.group(1)) - 1)
                        chapter_found = True
                        log.debug(f"Found chapter via path pattern: {chapter}")
                
                # Fallback: Estimate chapter from progress if no pattern matched
                if not chapter_found and bookmark.progress_percent:
                    # Use a higher chapter estimate to better match MoonReader's counting
                    # MoonReader seems to use finer chapter granularity
                    estimated_chapters = 50  # Increased from 40
                    chapter = int((bookmark.progress_percent / 100.0) * estimated_chapters)
                    chapter_found = True
                    log.info(f"Estimated chapter from progress: {chapter} (progress: {bookmark.progress_percent}%, using {estimated_chapters} max chapters)")
                
                # Extract position from kobo fragment
                kobo_match = re.search(r'#kobo\.(\d+)\.(\d+)', location_str)
                if kobo_match:
                    kobo_chapter_ref = int(kobo_match.group(1))
                    kobo_position = int(kobo_match.group(2))
                    
                    # If we found a chapter reference in the kobo fragment, use it as fallback
                    if not chapter_found and kobo_chapter_ref > 0:
                        chapter = max(0, kobo_chapter_ref - 1)
                        log.debug(f"Using kobo fragment chapter reference: {chapter}")
                    
                    # For MoonReader scrolled_chars: this should represent how much has been 
                    # scrolled past in the current view, NOT total book progress
                    # If we're at a chapter boundary or start, this should be 0
                    # Only use kobo_position if it's small (indicating position within chapter)
                    if kobo_position < 5000:  # Arbitrary threshold for "within chapter"
                        scrolled_chars = kobo_position // 10  # Scale down significantly
                        screen_offset = kobo_position % 100
                    else:
                        # Large kobo_position might indicate we're at chapter start
                        scrolled_chars = 0
                        screen_offset = 0
                else:
                    # No kobo fragment found - assume we're at chapter start
                    scrolled_chars = 0
                    screen_offset = 0
                    
                log.debug(f"Kepub parsed - chapter: {chapter}, scrolled_chars: {scrolled_chars}, screen_offset: {screen_offset}")
                    
            except (ValueError, AttributeError) as e:
                log.info(f"Failed to parse .kepub location_value '{bookmark.location_value}': {e}")
                # Fallback: estimate chapter from progress
                if bookmark.progress_percent:
                    chapter = int((bookmark.progress_percent / 100.0) * 50)
                    log.info(f"Exception fallback - estimated chapter from progress: {chapter} (progress: {bookmark.progress_percent}%)")
                else:
                    chapter = 0
                scrolled_chars = 0
                screen_offset = 0
        else:
            # Parse .epub format (when MoonReader updates are coming back to Kobo)
            try:
                log.info(f"Parsing epub location: {bookmark.location_value}")
                
                chapter_found = False
                
                if '/' in str(bookmark.location_value):
                    parts = str(bookmark.location_value).split('/')
                    if parts[0].isdigit():
                        chapter = max(0, int(parts[0]) - 1)
                        chapter_found = True
                    
                    if len(parts) > 1 and parts[1].isdigit():
                        position = int(parts[1])
                        # For epub, keep scrolled_chars small - it's not total book progress
                        scrolled_chars = position // 100  # Much smaller scaling
                        screen_offset = position % 100
                else:
                    # Single number format
                    position_match = re.search(r'\d+', str(bookmark.location_value))
                    if position_match:
                        position = int(position_match.group())
                        scrolled_chars = position // 50  # Keep small
                        screen_offset = position % 50
                
                # Fallback: estimate chapter from progress if not found
                if not chapter_found and bookmark.progress_percent:
                    chapter = int((bookmark.progress_percent / 100.0) * 50)
                    log.info(f"Epub fallback - estimated chapter from progress: {chapter} (progress: {bookmark.progress_percent}%)")
                        
                log.info(f"Epub parsed - chapter: {chapter}, scrolled_chars: {scrolled_chars}, screen_offset: {screen_offset}")
                        
            except (ValueError, AttributeError) as e:
                log.info(f"Failed to parse epub location_value '{bookmark.location_value}': {e}")
                # Even in exception, try progress estimation
                if bookmark.progress_percent:
                    chapter = int((bookmark.progress_percent / 100.0) * 50)
                    log.info(f"Epub exception fallback - estimated chapter from progress: {chapter} (progress: {bookmark.progress_percent}%)")
                else:
                    chapter = 0
                scrolled_chars = 0
                screen_offset = 0
    else:
        # No location_value at all - estimate from progress if available
        if bookmark.progress_percent:
            chapter = int((bookmark.progress_percent / 100.0) * 40)
            log.info(f"No location_value - estimated chapter from progress: {chapter}")
        else:
            chapter = 0
            log.info("No location_value and no progress - defaulting to chapter 0")
    
    # Progress percentage - MoonReader uses CHAPTER progress, not book progress
    # Since Kobo's progress_percent is likely for the entire book, we need to estimate chapter progress
    # This is challenging without knowing the actual chapter structure
    
    if bookmark.progress_percent:
        # Try to estimate chapter progress from book progress and chapter number
        book_progress = bookmark.progress_percent
        
        # If we know the chapter and have position info, try to estimate chapter progress
        if chapter > 0:
            # Rough estimate: if we're in chapter N, assume each chapter is roughly equal
            # and calculate where we are within this chapter
            estimated_chapters = 50  # Same as our chapter estimation
            chapter_start_percent = (chapter / estimated_chapters) * 100
            chapter_end_percent = ((chapter + 1) / estimated_chapters) * 100
            
            # Calculate progress within this chapter
            if book_progress >= chapter_start_percent:
                chapter_range = chapter_end_percent - chapter_start_percent
                progress_in_range = book_progress - chapter_start_percent
                chapter_progress = (progress_in_range / chapter_range) * 100
                
                # Clamp to reasonable values
                chapter_progress = max(0.0, min(100.0, chapter_progress))
            else:
                # We're somehow before this chapter start, assume beginning of chapter
                chapter_progress = 0.0
        else:
            # Chapter 0 or unknown, assume early in chapter
            chapter_progress = min(book_progress * 2, 100.0)  # Scale up for early chapters
        
        progress = chapter_progress
        log.info(f"Converted book progress {book_progress}% to chapter {chapter} progress {progress:.1f}%")
    else:
        progress = 0.0
    
    # Final sanity check: scrolled_chars should be small for MoonReader
    if scrolled_chars > 10000:  # Arbitrary large number threshold
        log.debug(f"Scrolled chars too large ({scrolled_chars}), resetting to 0")
        scrolled_chars = 0
    
    log.info(f"Final MoonReader position: {timestamp}*{chapter}@{scrolled_chars}#{screen_offset}:{progress:.1f}%")
    return f"{timestamp}*{chapter}@{scrolled_chars}#{screen_offset}:{progress:.1f}%"

def moonreader_to_kobo_position(moonreader_content, book=None):
    """Parse Moonreader .epub.po format back to Kobo position data.
    
    Note: MoonReader percentage is chapter progress, not book progress.
    We need to convert this back to estimated book progress for Kobo.
    """
    if not moonreader_content or not moonreader_content.strip():
        return None
    
    try:
        content = moonreader_content.strip()
        log.debug(f"Parsing MoonReader content: {content}")
        
        # Parse format: timestamp*chapter@scrolled_chars#screen_offset:percentage%
        parts = re.match(r'(\d+)\*(\d+)@(\d+)#(\d+):([0-9.]+)%', content)
        if not parts:
            log.warning(f"Invalid Moonreader position format: {content}")
            return None
        
        timestamp_ms, chapter, scrolled_chars, screen_offset, progress = parts.groups()
        
        # Convert zero-based chapter to 1-based for Kobo
        kobo_chapter = int(chapter) + 1
        
        # Convert scrolled characters back to position more intelligently
        total_content_length = get_epub_content_length(book)
        progress_ratio = float(progress) / 100.0
        
        # Estimate position based on scrolled characters and screen offset
        # The scrolled_chars represents content already read, so we add screen_offset
        estimated_char_position = int(scrolled_chars) + int(screen_offset)
        
        # Convert to a position relative to chapter
        # This is still an approximation, but better than the original
        chapter_relative_position = estimated_char_position % 10000  # Assume max 10k chars per chapter
        
        # Convert chapter progress back to estimated book progress
        estimated_chapters = 50  # Same as our chapter estimation
        chapter_progress = float(progress)
        
        # Calculate estimated book progress from chapter and chapter progress
        chapter_start_percent = (int(chapter) / estimated_chapters) * 100
        chapter_range_percent = (1 / estimated_chapters) * 100
        book_progress = chapter_start_percent + (chapter_progress / 100.0) * chapter_range_percent
        
        # Clamp to reasonable values
        book_progress = max(0.0, min(100.0, book_progress))
        
        log.debug(f"Parsed - chapter: {chapter} (MR) -> {kobo_chapter} (Kobo), chapter_progress: {chapter_progress}% -> book_progress: {book_progress:.1f}%")
        
        # Detect book format for proper location_value generation
        book_format = get_book_format(book) if book else 'epub'
        
        if book_format == 'kepub':
            # Generate .kepub ContentID format
            book_filename = sanitize_filename(book.title) if book and hasattr(book, 'title') else 'book'
            
            # Use the estimated position in the kobo format
            location_value = f"{book_filename}.kepub.epub!OEBPS/Text/Chapter{kobo_chapter}.xhtml#kobo.{kobo_chapter}.{chapter_relative_position}"
            location_type = 'kobo'
            location_source = 'calibre-web'
        else:
            # Use simple .epub format
            location_value = f"{kobo_chapter}/{chapter_relative_position}"
            location_type = 'text'
            location_source = 'moonreader'
        
        log.debug(f"Generated Kobo location_value: {location_value}")
        
        return {
            'timestamp': datetime.fromtimestamp(int(timestamp_ms) / 1000, timezone.utc),
            'progress_percent': book_progress,  # Use converted book progress, not chapter progress
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