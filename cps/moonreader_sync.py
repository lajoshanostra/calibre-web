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
from .book_analyzer import book_analyzer
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
    """Convert Kobo reading state to Moonreader .epub.po format using file-based analysis.
    
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
    
    log.info(f"Converting Kobo position - location: '{bookmark.location_value}', source: '{bookmark.location_source}', progress: {bookmark.progress_percent}%")
    
    # Use the new source-aware parsing approach
    position_info = None
    if book and bookmark.location_value:
        try:
            # Parse Kobo location with source file information
            kobo_location_info = book_analyzer.parse_kobo_location_with_source(
                str(bookmark.location_value), 
                bookmark.location_source
            )
            
            if kobo_location_info and (kobo_location_info.get('source_chapter') or kobo_location_info.get('source_split')):
                span_x = kobo_location_info['span_x']
                span_y = kobo_location_info['span_y']
                
                # Handle different source file formats
                if kobo_location_info.get('source_chapter'):
                    # Direct Chapter format (e.g., Chapter09.xhtml)
                    source_chapter = kobo_location_info['source_chapter']
                    chapter = source_chapter - 1  # Convert to 0-based
                    log.info(f"Using direct chapter mapping: Chapter{source_chapter} → chapter {chapter}")
                    
                elif kobo_location_info.get('source_split'):
                    # Split file format (e.g., The_Stranger_split_7.html)
                    split_number = kobo_location_info['source_split']
                    toc_index = book_analyzer.map_split_to_chapter(split_number, book)
                    
                    if toc_index is not None:
                        # MoonReader expects 1-based chapter numbering
                        # TOC index is 0-based, so add 1
                        chapter = toc_index + 1
                        log.info(f"Mapped split_{split_number} to TOC index {toc_index} → MoonReader chapter {chapter}")
                    else:
                        log.warning(f"Could not map split_{split_number} to chapter, using fallback")
                        chapter = 1  # Fallback to chapter 1
                else:
                    chapter = 0  # Fallback
                
                # Estimate position within chapter based on span numbers
                # This is a rough estimate - could be improved with more analysis
                scrolled_chars = span_x * 10  # Simple estimation
                screen_offset = span_y
                
                # Calculate chapter progress from book progress and chapter position
                # If we know total chapters, we can calculate chapter progress
                try:
                    file_paths = book_analyzer.get_book_files(book)
                    if 'epub' in file_paths:
                        epub_structure = book_analyzer.analyze_epub_structure(file_paths['epub'])
                        if epub_structure and epub_structure.total_chapters > 0:
                            total_chapters = epub_structure.total_chapters
                            chapter_size_percent = 100.0 / total_chapters
                            
                            # Calculate chapter start percentage
                            chapter_start_percent = chapter * chapter_size_percent
                            
                            # Calculate how much of this chapter we've read
                            if bookmark.progress_percent > chapter_start_percent:
                                chapter_progress = ((bookmark.progress_percent - chapter_start_percent) / chapter_size_percent) * 100
                                chapter_progress = max(0.0, min(100.0, chapter_progress))
                            else:
                                chapter_progress = 0.0
                        else:
                            # Fallback estimation
                            chapter_progress = 10.0  # Conservative estimate
                    else:
                        chapter_progress = 10.0
                except Exception:
                    chapter_progress = 10.0
                
                # Create appropriate log message
                if kobo_location_info.get('source_chapter'):
                    log.info(f"Source-based analysis: Chapter{kobo_location_info['source_chapter']} → chapter {chapter}, span {span_x}.{span_y}, progress {chapter_progress:.1f}%")
                elif kobo_location_info.get('source_split'):
                    log.info(f"Source-based analysis: split_{kobo_location_info['source_split']} → chapter {chapter}, span {span_x}.{span_y}, progress {chapter_progress:.1f}%")
                
            else:
                # Fallback to original analysis method
                progress_percent = bookmark.progress_percent if bookmark.progress_percent else None
                position_info = book_analyzer.map_kobo_position_to_epub(str(bookmark.location_value), book, progress_percent)
                if position_info:
                    chapter = position_info.chapter
                    scrolled_chars = position_info.paragraph
                    screen_offset = position_info.character_offset
                    chapter_progress = position_info.chapter_progress
                    log.info(f"File-based analysis successful - chapter: {chapter}, progress: {chapter_progress:.1f}%")
                else:
                    # Final fallback
                    chapter, scrolled_chars, screen_offset, chapter_progress = _estimate_position_fallback(bookmark, book)
                    log.warning(f"Using fallback estimation")
        except Exception as e:
            log.warning(f"Source-aware analysis failed: {e}, falling back to estimation")
            chapter, scrolled_chars, screen_offset, chapter_progress = _estimate_position_fallback(bookmark, book)
    
    log.info(f"Final position - chapter: {chapter}, scrolled_chars: {scrolled_chars}, screen_offset: {screen_offset}, chapter_progress: {chapter_progress:.1f}%")
    return f"{timestamp}*{chapter}@{scrolled_chars}#{screen_offset}:{chapter_progress:.1f}%"


def _estimate_position_fallback(bookmark, book=None):
    """Fallback position estimation when file analysis fails."""
    chapter = 0
    scrolled_chars = 0
    screen_offset = 0
    chapter_progress = 0.0
    
    # Detect book format for proper location parsing
    book_format = get_book_format(book) if book else 'epub'
    
    if bookmark.location_value:
        # Simplified fallback - basic chapter estimation from progress
        if bookmark.progress_percent:
            estimated_chapters = 50  # Default estimation
            chapter = int((bookmark.progress_percent / 100.0) * estimated_chapters)
            log.info(f"Fallback estimation - chapter: {chapter} from progress: {bookmark.progress_percent}%")
        
        # Extract basic position info if available
        if book_format == 'kepub' and '!' in str(bookmark.location_value):
            kobo_match = re.search(r'#kobo\.(\d+)\.(\d+)', str(bookmark.location_value))
            if kobo_match:
                kobo_position = int(kobo_match.group(2))
                scrolled_chars = min(kobo_position // 100, 50)  # Keep small
                screen_offset = kobo_position % 100
        elif '/' in str(bookmark.location_value):
            parts = str(bookmark.location_value).split('/')
            if len(parts) >= 2 and parts[1].isdigit():
                position = int(parts[1])
                scrolled_chars = min(position // 100, 50)
                screen_offset = position % 100
    else:
        # No location_value - estimate from progress only
        if bookmark.progress_percent:
            chapter = int((bookmark.progress_percent / 100.0) * 50)
            log.info(f"No location_value - estimated chapter: {chapter} from progress: {bookmark.progress_percent}%")
    
    # Estimate chapter progress from book progress (simplified fallback)
    if bookmark.progress_percent and chapter >= 0:
        estimated_chapters = 50
        chapter_start_percent = (chapter / estimated_chapters) * 100
        chapter_end_percent = ((chapter + 1) / estimated_chapters) * 100
        chapter_range = chapter_end_percent - chapter_start_percent
        
        if bookmark.progress_percent >= chapter_start_percent:
            progress_in_range = bookmark.progress_percent - chapter_start_percent
            chapter_progress = (progress_in_range / chapter_range) * 100
            chapter_progress = max(0.0, min(100.0, chapter_progress))
        else:
            chapter_progress = 0.0
    else:
        chapter_progress = 0.0
    
    return chapter, scrolled_chars, screen_offset, chapter_progress

def moonreader_to_kobo_position(moonreader_content, book=None):
    """Parse Moonreader .epub.po format back to Kobo position data using file-based analysis.
    
    Note: MoonReader percentage is chapter progress, not book progress.
    We need to convert this back to estimated book progress for Kobo.
    """
    if not moonreader_content or not moonreader_content.strip():
        return None
    
    try:
        content = moonreader_content.strip()
        log.info(f"Parsing MoonReader content: {content}")
        
        # Parse format: timestamp*chapter@scrolled_chars#screen_offset:percentage%
        parts = re.match(r'(\d+)\*(\d+)@(\d+)#(\d+):([0-9.]+)%', content)
        if not parts:
            log.warning(f"Invalid Moonreader position format: {content}")
            return None
        
        timestamp_ms, chapter, scrolled_chars, screen_offset, chapter_progress = parts.groups()
        
        # Try file-based analysis for accurate chapter count
        actual_chapters = None
        if book:
            try:
                file_paths = book_analyzer.get_book_files(book)
                if 'epub' in file_paths:
                    epub_structure = book_analyzer.analyze_epub_structure(file_paths['epub'])
                    if epub_structure:
                        actual_chapters = epub_structure.total_chapters
                        log.info(f"File-based analysis found {actual_chapters} chapters")
            except Exception as e:
                log.warning(f"File-based chapter analysis failed: {e}")
        
        # Use actual chapter count if available, otherwise fall back to estimation
        estimated_chapters = actual_chapters if actual_chapters else 50
        
        # Convert chapter progress back to estimated book progress
        chapter_num = int(chapter)
        chapter_progress_val = float(chapter_progress)
        
        # Calculate estimated book progress from chapter and chapter progress
        chapter_start_percent = (chapter_num / estimated_chapters) * 100
        chapter_range_percent = (1 / estimated_chapters) * 100
        book_progress = chapter_start_percent + (chapter_progress_val / 100.0) * chapter_range_percent
        
        # Clamp to reasonable values
        book_progress = max(0.0, min(100.0, book_progress))
        
        log.info(f"Converted MR chapter {chapter_num} ({chapter_progress_val}%) to book progress {book_progress:.1f}% (using {estimated_chapters} total chapters)")
        
        # Detect book format for proper location_value generation
        book_format = get_book_format(book) if book else 'epub'
        
        if book_format == 'kepub':
            # Generate .kepub ContentID format using algorithmic approach
            try:
                file_paths = book_analyzer.get_book_files(book)
                if 'kepub' in file_paths:
                    # Count total spans and map progress to actual position
                    total_spans = book_analyzer.count_total_kobo_spans(file_paths['kepub'])
                    if total_spans > 0:
                        # Calculate target span based on book progress
                        target_span_number = int((book_progress / 100.0) * total_spans)
                        target_span_number = max(1, min(total_spans, target_span_number))  # Clamp to valid range
                        
                        # Map progress to actual EPUB chapter
                        epub_chapter = book_analyzer.map_progress_to_epub_chapter(book_progress, book)
                        if epub_chapter is not None:
                            # Generate valid location using actual chapter and span
                            book_filename = sanitize_filename(book.title) if book and hasattr(book, 'title') else 'book'
                            location_value = f"{book_filename}.kepub.epub!OEBPS/Text/Chapter{epub_chapter + 1:02d}.xhtml#kobo.{target_span_number}.1"
                            log.info(f"Algorithmic mapping: {book_progress:.1f}% → span {target_span_number}/{total_spans} → Chapter{epub_chapter + 1:02d}")
                        else:
                            # Fallback to simple format
                            location_value = f"kobo.{target_span_number}.1"
                            log.warning(f"Could not map to EPUB chapter, using simple span reference: {location_value}")
                    else:
                        # Fallback if span counting fails
                        location_value = f"kobo.1.1"
                        log.warning("Could not count spans, using default position")
                else:
                    # Fallback if KEPUB file not found
                    location_value = f"kobo.1.1"
                    log.warning("KEPUB file not found, using default position")
            except Exception as e:
                log.error(f"Error in algorithmic mapping: {e}")
                location_value = f"kobo.1.1"
            
            location_type = 'KoboSpan'
            location_source = 'calibre-web'
        else:
            # Use simple .epub format - use actual chapter number from EPUB structure
            try:
                epub_chapter = book_analyzer.map_progress_to_epub_chapter(book_progress, book)
                if epub_chapter is not None:
                    kobo_chapter = epub_chapter + 1  # Convert to 1-based
                else:
                    kobo_chapter = int(chapter) + 1  # Fallback to original logic
            except:
                kobo_chapter = int(chapter) + 1  # Fallback to original logic
            
            estimated_position = int(scrolled_chars) * 100 + int(screen_offset)
            location_value = f"{kobo_chapter}/{estimated_position}"
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