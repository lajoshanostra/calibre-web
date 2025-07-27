# -*- coding: utf-8 -*-

"""
Book File Analysis Module for Cross-Format Position Mapping

This module provides functionality to analyze .epub and .kepub files to determine
accurate chapter structures and position mappings for MoonReader sync.
"""

import os
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple, NamedTuple
import tempfile
import shutil
import difflib
from . import logger, config

log = logger.create()

# Try to import BeautifulSoup for HTML parsing, fall back gracefully
try:
    from bs4 import BeautifulSoup
    HAS_BEAUTIFULSOUP = True
except ImportError:
    log.warning("BeautifulSoup4 not available - falling back to basic XML parsing")
    HAS_BEAUTIFULSOUP = False


class ChapterInfo(NamedTuple):
    """Information about a chapter in a book."""
    number: int
    title: str
    file_path: str
    start_position: int = 0
    length: int = 0


class BookStructure(NamedTuple):
    """Complete structure information for a book."""
    chapters: List[ChapterInfo]
    total_chapters: int
    format_type: str  # 'epub' or 'kepub'
    spine_order: List[str] = []


class PositionInfo(NamedTuple):
    """Position information within a book."""
    chapter: int
    paragraph: int
    character_offset: int
    chapter_progress: float  # 0-100%
    part: Optional[int] = None
    chapter_in_part: Optional[int] = None
    is_part_header: bool = False


class TextSample(NamedTuple):
    """Text sample extracted from a specific position in a book."""
    text: str
    chapter_file: str
    position_in_file: int
    surrounding_context: str  # Larger text block for better matching


class BookAnalyzer:
    """Analyzes book files to extract chapter structure and position information."""
    
    def __init__(self):
        self.cache = {}  # Simple in-memory cache for book structures
    
    def _get_library_path(self) -> Optional[str]:
        """Get the Calibre library path with fallback options."""
        try:
            # First try: Use Calibre-Web's configuration
            if hasattr(config, 'config_calibre_dir') and config.config_calibre_dir:
                library_path = config.config_calibre_dir
                if os.path.exists(library_path):
                    log.debug(f"Using configured library path: {library_path}")
                    return library_path
        except Exception as e:
            log.debug(f"Could not get library path from config: {e}")
        
        # Fallback options
        fallback_paths = [
            '/opt/calibre-web/library',  # Most common deployment path
            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'library'),  # Relative to cps module
            os.path.join(os.getcwd(), 'library'),  # Current working directory
            '/usr/local/calibre-web/library',  # Alternative deployment path
        ]
        
        for path in fallback_paths:
            if os.path.exists(path) and os.path.isdir(path):
                log.debug(f"Using fallback library path: {path}")
                return path
        
        log.error(f"Could not find library directory. Tried: {fallback_paths}")
        return None
    
    def get_book_files(self, book) -> Dict[str, str]:
        """Get available file formats for a book from Calibre-Web."""
        file_paths = {}
        
        if not book or not hasattr(book, 'path') or not hasattr(book, 'data'):
            return file_paths
        
        try:
            # Get library path from Calibre-Web configuration
            library_path = self._get_library_path()
            if not library_path:
                log.warning("Could not determine library path")
                return file_paths
            
            # Construct book directory path
            book_dir = os.path.join(library_path, book.path)
            
            if not os.path.exists(book_dir):
                log.info(f"Book directory not found: {book_dir} (library_path: {library_path}, book.path: {book.path})")
                return file_paths
            
            # Look for available formats
            for data_entry in book.data:
                format_name = data_entry.format.lower()
                if format_name in ['epub', 'kepub']:
                    # Try multiple filename variations for kepub files
                    if format_name == 'kepub':
                        # Try both common kepub extensions
                        possible_filenames = [
                            f"{data_entry.name}.kepub.epub",  # Standard kepub extension
                            f"{data_entry.name}.kepub",       # Alternative kepub extension
                        ]
                    else:
                        possible_filenames = [f"{data_entry.name}.{format_name}"]
                    
                    # Check which file actually exists
                    file_found = False
                    for filename in possible_filenames:
                        file_path = os.path.join(book_dir, filename)
                        if os.path.exists(file_path):
                            file_paths[format_name] = file_path
                            log.debug(f"Found {format_name} file: {file_path}")
                            file_found = True
                            break
                    
                    # Log if we couldn't find the expected format
                    if not file_found:
                        log.debug(f"Could not find {format_name} file. Tried: {[os.path.join(book_dir, f) for f in possible_filenames]}")
            
            # Fallback: Scan directory for any epub/kepub files if database entries didn't work
            if not file_paths:
                log.debug(f"No files found via database entries, scanning directory: {book_dir}")
                try:
                    for filename in os.listdir(book_dir):
                        lower_filename = filename.lower()
                        if lower_filename.endswith('.epub') and 'epub' not in file_paths:
                            file_paths['epub'] = os.path.join(book_dir, filename)
                            log.debug(f"Found epub file via directory scan: {filename}")
                        elif (lower_filename.endswith('.kepub.epub') or lower_filename.endswith('.kepub')) and 'kepub' not in file_paths:
                            file_paths['kepub'] = os.path.join(book_dir, filename)
                            log.debug(f"Found kepub file via directory scan: {filename}")
                except OSError as e:
                    log.debug(f"Could not scan directory {book_dir}: {e}")
            
        except Exception as e:
            log.error(f"Error finding book files for {book.title}: {e}")
        
        return file_paths
    
    def analyze_epub_structure(self, epub_path: str) -> Optional[BookStructure]:
        """Analyze EPUB file to extract chapter structure."""
        try:
            with zipfile.ZipFile(epub_path, 'r') as epub_zip:
                # Find content.opf file
                opf_path = self._find_opf_file(epub_zip)
                if not opf_path:
                    log.warning(f"Could not find OPF file in {epub_path}")
                    return None
                
                # Parse OPF to get spine and manifest
                opf_content = epub_zip.read(opf_path).decode('utf-8')
                spine_order, manifest = self._parse_opf(opf_content)
                
                # Find navigation document (TOC)
                toc_items = self._find_toc_items(epub_zip, opf_path, manifest)
                
                # Build chapter structure
                chapters = []
                for i, (title, href) in enumerate(toc_items):
                    chapter_info = ChapterInfo(
                        number=i,
                        title=title,
                        file_path=href
                    )
                    chapters.append(chapter_info)
                
                # If no TOC found, use spine order as chapters
                if not chapters:
                    for i, spine_item in enumerate(spine_order[:20]):  # Limit to reasonable number
                        if spine_item in manifest:
                            chapter_info = ChapterInfo(
                                number=i,
                                title=f"Chapter {i+1}",
                                file_path=manifest[spine_item]
                            )
                            chapters.append(chapter_info)
                
                return BookStructure(
                    chapters=chapters,
                    total_chapters=len(chapters),
                    format_type='epub',
                    spine_order=spine_order
                )
                
        except Exception as e:
            log.error(f"Error analyzing EPUB structure for {epub_path}: {e}")
            return None
    
    def analyze_kepub_structure(self, kepub_path: str) -> Optional[BookStructure]:
        """Analyze KEPUB file to extract chapter structure and position mappings."""
        try:
            with zipfile.ZipFile(kepub_path, 'r') as kepub_zip:
                # KEPUB files have the same basic structure as EPUB
                # but with additional kobo spans for position tracking
                opf_path = self._find_opf_file(kepub_zip)
                if not opf_path:
                    return None
                
                opf_content = kepub_zip.read(opf_path).decode('utf-8')
                spine_order, manifest = self._parse_opf(opf_content)
                
                # For KEPUB, we can also analyze the kobo spans
                chapters = []
                for i, spine_item in enumerate(spine_order):
                    if spine_item in manifest:
                        file_path = manifest[spine_item]
                        
                        # Try to read content and analyze kobo spans
                        try:
                            content = kepub_zip.read(file_path).decode('utf-8')
                            chapter_title = self._extract_chapter_title(content)
                            
                            chapter_info = ChapterInfo(
                                number=i,
                                title=chapter_title or f"Chapter {i+1}",
                                file_path=file_path
                            )
                            chapters.append(chapter_info)
                        except:
                            # Skip problematic files
                            continue
                
                return BookStructure(
                    chapters=chapters,
                    total_chapters=len(chapters),
                    format_type='kepub',
                    spine_order=spine_order
                )
                
        except Exception as e:
            log.error(f"Error analyzing KEPUB structure for {kepub_path}: {e}")
            return None
    
    def map_kobo_position_to_epub(self, kobo_location: str, book, progress_percent: float = None) -> Optional[PositionInfo]:
        """Map a Kobo location to equivalent position in EPUB format using content-based analysis."""
        try:
            file_paths = self.get_book_files(book)
            
            if 'kepub' not in file_paths or 'epub' not in file_paths:
                log.debug("Missing required file formats for position mapping")
                return None
            
            log.debug(f"Starting content-based position mapping for {kobo_location}")
            
            # Step 1: Extract text sample from KEPUB at the Kobo location
            kepub_text_sample = self.extract_text_from_kepub(file_paths['kepub'], kobo_location, progress_percent)
            if not kepub_text_sample:
                log.debug("Could not extract text sample from KEPUB, falling back to structural mapping")
                return self._fallback_structural_mapping(kobo_location, file_paths)
            
            log.debug(f"Extracted text sample from KEPUB: '{kepub_text_sample.text[:50]}...'")
            
            # Step 2: Search for the same text in EPUB
            epub_position = self.search_text_in_epub(
                file_paths['epub'], 
                kepub_text_sample.text, 
                kepub_text_sample.surrounding_context
            )
            
            if epub_position:
                # Convert EPUB chapter to Part/Chapter format
                enhanced_position = self._enhance_position_with_part_chapter(epub_position, file_paths['epub'])
                if enhanced_position:
                    return enhanced_position
                else:
                    log.info(f"Content-based mapping successful: KEPUB {kobo_location} → EPUB chapter {epub_position.chapter}, progress {epub_position.chapter_progress:.1f}%")
                    return epub_position
            else:
                log.debug("Text not found in EPUB, trying fallback with context")
                # Try searching with just the surrounding context
                if len(kepub_text_sample.surrounding_context) > 50:
                    context_words = kepub_text_sample.surrounding_context.split()
                    if len(context_words) > 10:
                        # Try with middle portion of context
                        middle_context = ' '.join(context_words[len(context_words)//4:3*len(context_words)//4])
                        epub_position = self.search_text_in_epub(file_paths['epub'], middle_context)
                        if epub_position:
                            # Convert EPUB chapter to Part/Chapter format
                            enhanced_position = self._enhance_position_with_part_chapter(epub_position, file_paths['epub'])
                            if enhanced_position:
                                return enhanced_position
                            else:
                                log.info(f"Context-based mapping successful: {kobo_location} → EPUB chapter {epub_position.chapter}")
                                return epub_position
                
                log.debug("Content-based mapping failed, falling back to structural mapping")
                return self._fallback_structural_mapping(kobo_location, file_paths)
            
        except Exception as e:
            log.error(f"Error in content-based mapping for {kobo_location}: {e}")
            return self._fallback_structural_mapping(kobo_location, file_paths)
    
    def _fallback_structural_mapping(self, kobo_location: str, file_paths: Dict[str, str]) -> Optional[PositionInfo]:
        """Fallback to structural mapping when content-based mapping fails."""
        try:
            # Analyze both file structures
            kepub_structure = self.analyze_kepub_structure(file_paths['kepub'])
            epub_structure = self.analyze_epub_structure(file_paths['epub'])
            
            if not kepub_structure or not epub_structure:
                return None
            
            # Parse Kobo location to extract chapter and position
            chapter_num, position = self._parse_kobo_location(kobo_location, kepub_structure)
            
            if chapter_num is None:
                return None
            
            # Map to equivalent EPUB chapter (capped at actual chapter count)
            epub_chapter = min(chapter_num, epub_structure.total_chapters - 1)
            
            # Improved position calculation
            if position > 100:
                # Large position suggests it's a character offset, scale it down
                chapter_progress = min((position / 10000.0) * 100, 100.0)
            else:
                # Small position suggests it's already a reasonable offset
                chapter_progress = min((position / 100.0) * 100, 100.0)
            
            log.debug(f"Structural mapping: {kobo_location} → chapter {epub_chapter}, progress {chapter_progress:.1f}%")
            
            return PositionInfo(
                chapter=epub_chapter,
                paragraph=position // 50,  # Rough estimate
                character_offset=position % 50,
                chapter_progress=chapter_progress
            )
            
        except Exception as e:
            log.error(f"Error in structural mapping: {e}")
            return None
    
    def _find_opf_file(self, zip_file: zipfile.ZipFile) -> Optional[str]:
        """Find the OPF (package) file in an EPUB/KEPUB."""
        # Look for META-INF/container.xml first
        try:
            container_content = zip_file.read('META-INF/container.xml').decode('utf-8')
            root = ET.fromstring(container_content)
            
            # Find rootfile element
            for rootfile in root.findall('.//{urn:oasis:names:tc:opendocument:xmlns:container}rootfile'):
                full_path = rootfile.get('full-path')
                if full_path and full_path.endswith('.opf'):
                    return full_path
        except:
            pass
        
        # Fallback: look for .opf files directly
        for filename in zip_file.namelist():
            if filename.endswith('.opf'):
                return filename
        
        return None
    
    def _parse_opf(self, opf_content: str) -> Tuple[List[str], Dict[str, str]]:
        """Parse OPF content to extract spine order and manifest."""
        spine_order = []
        manifest = {}
        
        try:
            root = ET.fromstring(opf_content)
            
            # Parse manifest
            for item in root.findall('.//{http://www.idpf.org/2007/opf}item'):
                item_id = item.get('id')
                href = item.get('href')
                if item_id and href:
                    manifest[item_id] = href
            
            # Parse spine
            for itemref in root.findall('.//{http://www.idpf.org/2007/opf}itemref'):
                idref = itemref.get('idref')
                if idref:
                    spine_order.append(idref)
        
        except Exception as e:
            log.error(f"Error parsing OPF content: {e}")
        
        return spine_order, manifest
    
    def _find_toc_items(self, zip_file: zipfile.ZipFile, opf_path: str, manifest: Dict[str, str]) -> List[Tuple[str, str]]:
        """Find table of contents items."""
        toc_items = []
        
        # Try to find navigation document
        nav_file = None
        
        # Look for HTML5 nav document first
        for item_id, href in manifest.items():
            if 'nav' in href.lower() or 'toc' in href.lower():
                nav_file = href
                break
        
        # Try to parse navigation
        if nav_file:
            try:
                nav_content = zip_file.read(nav_file).decode('utf-8')
                toc_items = self._parse_nav_html(nav_content)
            except:
                pass
        
        # Fallback: look for NCX file
        if not toc_items:
            try:
                toc_items = self._find_ncx_toc(zip_file, manifest, opf_path)
            except:
                pass
        
        return toc_items
    
    def _parse_nav_html(self, nav_content: str) -> List[Tuple[str, str]]:
        """Parse HTML5 navigation document."""
        toc_items = []
        
        if not HAS_BEAUTIFULSOUP:
            return toc_items
        
        try:
            soup = BeautifulSoup(nav_content, 'html.parser')
            
            # Look for navigation elements
            nav_element = soup.find('nav')
            if nav_element:
                for link in nav_element.find_all('a'):
                    href = link.get('href')
                    title = link.get_text(strip=True)
                    if href and title:
                        toc_items.append((title, href))
        
        except Exception as e:
            log.error(f"Error parsing navigation HTML: {e}")
        
        return toc_items
    
    def _find_ncx_toc(self, zip_file: zipfile.ZipFile, manifest: Dict[str, str], opf_path: str) -> List[Tuple[str, str]]:
        """Find and parse NCX table of contents."""
        toc_items = []
        
        # Look for NCX file
        ncx_file = None
        for item_id, href in manifest.items():
            if href.endswith('.ncx'):
                ncx_file = href
                break
        
        if ncx_file:
            try:
                # NCX file path might be relative to OPF directory
                # Try both relative to OPF and absolute paths
                ncx_paths_to_try = [
                    ncx_file,  # As specified in manifest
                    os.path.join(os.path.dirname(opf_path), ncx_file),  # Relative to OPF directory
                ]
                
                ncx_content = None
                for ncx_path in ncx_paths_to_try:
                    try:
                        ncx_content = zip_file.read(ncx_path).decode('utf-8')
                        log.debug(f"Successfully read NCX file from: {ncx_path}")
                        break
                    except KeyError:
                        continue
                
                if not ncx_content:
                    log.warning(f"Could not find NCX file. Tried: {ncx_paths_to_try}")
                    return toc_items
                root = ET.fromstring(ncx_content)
                
                for navpoint in root.findall('.//{http://www.daisy.org/z3986/2005/ncx/}navPoint'):
                    # Extract title
                    text_elem = navpoint.find('.//{http://www.daisy.org/z3986/2005/ncx/}text')
                    title = text_elem.text if text_elem is not None else "Unknown"
                    
                    # Extract content src
                    content_elem = navpoint.find('.//{http://www.daisy.org/z3986/2005/ncx/}content')
                    href = content_elem.get('src') if content_elem is not None else None
                    
                    if title and href:
                        toc_items.append((title, href))
            
            except Exception as e:
                log.error(f"Error parsing NCX file: {e}")
        
        return toc_items
    
    def _extract_chapter_title(self, content: str) -> Optional[str]:
        """Extract chapter title from HTML content."""
        if not HAS_BEAUTIFULSOUP:
            return None
        
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for common chapter title elements
            for tag in ['h1', 'h2', 'h3', 'title']:
                element = soup.find(tag)
                if element:
                    return element.get_text(strip=True)
        
        except:
            pass
        
        return None
    
    def _parse_kobo_location(self, kobo_location: str, kepub_structure: BookStructure) -> Tuple[Optional[int], int]:
        """Parse Kobo location string to extract chapter and position."""
        try:
            if '!' in kobo_location:
                # KEPUB format: book.kepub.epub!OEBPS/Text/Chapter3.xhtml#kobo.3.1234
                parts = kobo_location.split('!')
                if len(parts) >= 2:
                    file_part = parts[1]
                    
                    # Extract chapter number from filename
                    chapter_match = re.search(r'[Cc]hapter(\d+)', file_part)
                    if chapter_match:
                        chapter_num = int(chapter_match.group(1)) - 1  # Convert to 0-based
                    else:
                        # Try numbered files
                        file_match = re.search(r'/(\d+)\.x?html', file_part)
                        chapter_num = int(file_match.group(1)) if file_match else 0
                    
                    # Extract position from kobo fragment
                    position = 0
                    if '#kobo.' in kobo_location:
                        kobo_match = re.search(r'#kobo\.(\d+)\.(\d+)', kobo_location)
                        if kobo_match:
                            position = int(kobo_match.group(2))
                    
                    return chapter_num, position
            
            # Handle simple kobo.X.Y format
            if kobo_location.startswith('kobo.') and kobo_location.count('.') == 2:
                parts = kobo_location.split('.')
                if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
                    chapter_num = int(parts[1]) - 1  # Convert to 0-based
                    position = int(parts[2])
                    log.debug(f"Parsed simple kobo format: chapter {chapter_num}, position {position}")
                    return chapter_num, position
            
            # Fallback for simple formats
            if '/' in kobo_location:
                parts = kobo_location.split('/')
                if len(parts) >= 2 and parts[0].isdigit():
                    chapter_num = int(parts[0]) - 1  # Convert to 0-based
                    position = int(parts[1]) if parts[1].isdigit() else 0
                    return chapter_num, position
        
        except Exception as e:
            log.error(f"Error parsing Kobo location '{kobo_location}': {e}")
        
        return None, 0
    
    def _parse_kobo_span_location(self, kobo_location: str) -> Tuple[Optional[str], int]:
        """Parse Kobo location to extract actual span ID that exists in KEPUB files."""
        try:
            # Handle simple kobo.X.Y format - this is the actual span ID!
            if kobo_location.startswith('kobo.') and kobo_location.count('.') == 2:
                parts = kobo_location.split('.')
                if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
                    # The kobo_location IS the span ID, no transformation needed
                    span_id = kobo_location  # e.g., "kobo.16.1"
                    offset = 0  # We'll handle offset differently for kobo.X.Y format
                    log.debug(f"Parsed span location: {kobo_location} → span_id: {span_id}")
                    return span_id, offset
            
            # Handle complex KEPUB ContentID format if needed
            # e.g., book.kepub.epub!OEBPS/Text/Chapter2.xhtml#kobo.2.1234
            if '#kobo.' in kobo_location:
                kobo_match = re.search(r'#kobo\.(\d+)\.(\d+)', kobo_location)
                if kobo_match:
                    # Extract the kobo.X.Y part as the span ID
                    span_id = f"kobo.{kobo_match.group(1)}.{kobo_match.group(2)}"
                    offset = 0
                    log.debug(f"Parsed complex span location: {kobo_location} → span_id: {span_id}")
                    return span_id, offset
            
        except Exception as e:
            log.error(f"Error parsing Kobo span location '{kobo_location}': {e}")
        
        return None, 0
    
    def _find_span_in_kepub(self, kepub_zip: zipfile.ZipFile, span_id: str, offset: int, progress_percent: float = None) -> Optional[Dict]:
        """Search for a specific kobo.X.Y span ID across all KEPUB content files, using progress to find the correct file."""
        try:
            # Get KEPUB structure
            opf_path = self._find_opf_file(kepub_zip)
            if not opf_path:
                return None
            
            opf_content = kepub_zip.read(opf_path).decode('utf-8')
            spine_order, manifest = self._parse_opf(opf_content)
            
            log.info(f"Searching for span ID '{span_id}' across {len(spine_order)} content files (progress: {progress_percent}%)")
            
            # First, collect all occurrences of the span ID
            all_matches = []
            
            # Search through all content files in the spine
            for file_index, spine_item in enumerate(spine_order):
                if spine_item not in manifest:
                    continue
                
                content_file = manifest[spine_item]
                
                try:
                    # Try different path variations
                    content_paths = [
                        content_file,
                        os.path.join(os.path.dirname(opf_path), content_file)
                    ]
                    
                    content = None
                    actual_path = None
                    for content_path in content_paths:
                        try:
                            content = kepub_zip.read(content_path).decode('utf-8')
                            actual_path = content_path
                            break
                        except KeyError:
                            continue
                    
                    if not content:
                        continue
                    
                    # Check if this file contains the target span ID
                    if f'id="{span_id}"' in content:
                        # Calculate file position in book (rough approximation)
                        file_position_percent = (file_index / len(spine_order)) * 100
                        
                        # Extract text from the span in this file
                        span_info = self._extract_text_from_kobo_span_content_only(content, span_id, actual_path)
                        if span_info:
                            all_matches.append({
                                'file': actual_path,
                                'file_index': file_index,
                                'file_position_percent': file_position_percent,
                                'span_info': span_info
                            })
                            log.debug(f"Found span '{span_id}' in {actual_path} (file position ~{file_position_percent:.1f}%)")
                
                except Exception as e:
                    log.debug(f"Error processing file {content_file}: {e}")
                    continue
            
            # Now choose the best match based on progress percentage
            if not all_matches:
                log.debug(f"Span ID '{span_id}' not found in any content file")
                return None
            
            log.info(f"Found {len(all_matches)} occurrences of span '{span_id}'")
            
            # If we have progress percentage, use it to find the most likely file
            if progress_percent is not None and len(all_matches) > 1:
                # Find the match closest to the reported progress
                best_match = min(all_matches, 
                    key=lambda m: abs(m['file_position_percent'] - progress_percent))
                log.info(f"Using span from {best_match['file']} (file pos {best_match['file_position_percent']:.1f}% vs progress {progress_percent}%)")
                return best_match['span_info']
            else:
                # Use the first match or prioritize content files over cover/front matter
                content_matches = [m for m in all_matches if any(keyword in m['file'].lower() 
                    for keyword in ['chapter', 'content', 'text', 'part'])]
                
                if content_matches:
                    log.info(f"Using first content file match: {content_matches[0]['file']}")
                    return content_matches[0]['span_info']
                else:
                    log.info(f"Using first available match: {all_matches[0]['file']}")
                    return all_matches[0]['span_info']
        
        except Exception as e:
            log.error(f"Error searching for span {span_id} in KEPUB: {e}")
            return None
    
    def count_total_kobo_spans(self, kepub_path: str) -> int:
        """Count total number of kobo.X.Y spans in a KEPUB file."""
        try:
            with zipfile.ZipFile(kepub_path, 'r') as kepub_zip:
                opf_path = self._find_opf_file(kepub_zip)
                if not opf_path:
                    return 0
                
                opf_content = kepub_zip.read(opf_path).decode('utf-8')
                spine_order, manifest = self._parse_opf(opf_content)
                
                total_spans = 0
                kobo_span_pattern = re.compile(r'<span[^>]*id="kobo\.(\d+)\.(\d+)"', re.IGNORECASE)
                
                for spine_item in spine_order:
                    if spine_item not in manifest:
                        continue
                    
                    content_file = manifest[spine_item]
                    try:
                        content_paths = [
                            content_file,
                            os.path.join(os.path.dirname(opf_path), content_file)
                        ]
                        
                        content = None
                        for content_path in content_paths:
                            try:
                                content = kepub_zip.read(content_path).decode('utf-8')
                                break
                            except KeyError:
                                continue
                        
                        if not content:
                            continue
                        
                        # Count kobo spans in this file
                        matches = kobo_span_pattern.findall(content)
                        file_span_count = len(matches)
                        total_spans += file_span_count
                        
                        log.debug(f"File {content_file}: {file_span_count} kobo spans")
                    
                    except Exception as e:
                        log.debug(f"Error processing file {content_file} for span counting: {e}")
                        continue
                
                log.info(f"Total kobo spans in {kepub_path}: {total_spans}")
                return total_spans
                
        except Exception as e:
            log.error(f"Error counting kobo spans in {kepub_path}: {e}")
            return 0
    
    def map_progress_to_epub_chapter(self, book_progress_percent: float, book) -> Optional[int]:
        """Map book progress percentage to actual EPUB chapter number."""
        try:
            file_paths = self.get_book_files(book)
            if 'epub' not in file_paths:
                return None
            
            epub_structure = self.analyze_epub_structure(file_paths['epub'])
            if not epub_structure or not epub_structure.chapters:
                return None
            
            # Calculate which chapter contains this progress percentage
            total_chapters = epub_structure.total_chapters
            chapter_size_percent = 100.0 / total_chapters
            
            # Find the chapter that contains this progress
            target_chapter = int(book_progress_percent / chapter_size_percent)
            
            # Clamp to valid range (0-based indexing)
            target_chapter = max(0, min(total_chapters - 1, target_chapter))
            
            log.debug(f"Mapped {book_progress_percent:.1f}% progress to chapter {target_chapter} (of {total_chapters} total)")
            return target_chapter
            
        except Exception as e:
            log.error(f"Error mapping progress to chapter: {e}")
            return None
    
    def parse_kobo_location_with_source(self, location_value: str, location_source: str = None) -> Optional[Dict]:
        """Parse Kobo location value along with source file information."""
        try:
            # Parse kobo.X.Y format
            span_match = re.match(r'kobo\.(\d+)\.(\d+)', str(location_value))
            if not span_match:
                log.debug(f"Could not parse Kobo span format: {location_value}")
                return None
            
            span_x, span_y = span_match.groups()
            
            result = {
                'span_x': int(span_x),
                'span_y': int(span_y),
                'source_file': location_source,
                'raw_location': location_value
            }
            
            # Extract chapter number from source file if available
            if location_source:
                # Try different filename patterns
                log.debug(f"Analyzing source file: {location_source}")
                
                # Pattern 1: Chapter09.xhtml
                if 'Chapter' in location_source:
                    chapter_match = re.search(r'Chapter(\d+)', location_source)
                    if chapter_match:
                        result['source_chapter'] = int(chapter_match.group(1))
                        log.debug(f"Found Chapter pattern: {result['source_chapter']}")
                
                # Pattern 2: The_Stranger_split_7.html
                elif '_split_' in location_source:
                    split_match = re.search(r'_split_(\d+)', location_source)
                    if split_match:
                        # Split numbers are usually 0-based, but we need to map to actual chapters
                        result['source_split'] = int(split_match.group(1))
                        log.debug(f"Found split pattern: {result['source_split']}")
                        # We'll handle split-to-chapter mapping separately
                
                # Pattern 3: Other possible formats could be added here
                else:
                    log.debug(f"No recognized pattern in source file: {location_source}")
            
            log.debug(f"Parsed Kobo location: {result}")
            return result
            
        except Exception as e:
            log.error(f"Error parsing Kobo location: {e}")
            return None
    
    def map_split_to_chapter(self, split_number: int, book) -> Optional[int]:
        """Map split file number to actual chapter using TOC information."""
        try:
            file_paths = self.get_book_files(book)
            if 'epub' not in file_paths:
                return None
            
            # Use existing _find_ncx_toc method to get TOC items
            with zipfile.ZipFile(file_paths['epub'], 'r') as epub_zip:
                # Find OPF file first
                opf_path = self._find_opf_file(epub_zip)
                if not opf_path:
                    log.warning("Could not find OPF file for split mapping")
                    return None
                
                opf_content = epub_zip.read(opf_path).decode('utf-8')
                spine_order, manifest = self._parse_opf(opf_content)
                
                # Use existing NCX parsing logic
                toc_items = self._find_ncx_toc(epub_zip, manifest, opf_path)
                target_split_file = f"_split_{split_number}.html"
                
                log.debug(f"Looking for {target_split_file} in {len(toc_items)} TOC items")
                
                # Find the first chapter that appears in this split file
                for i, (title, href) in enumerate(toc_items):
                    if target_split_file in href:
                        log.debug(f"Mapped split_{split_number} to chapter {i} via TOC: '{title}' → {href}")
                        return i
                
                # If no exact match, try a more flexible approach
                for i, (title, href) in enumerate(toc_items):
                    if f'split_{split_number}' in href:
                        log.debug(f"Mapped split_{split_number} to chapter {i} via flexible matching: '{title}' → {href}")
                        return i
            
            log.warning(f"Could not map split_{split_number} to chapter")
            return None
            
        except Exception as e:
            log.error(f"Error mapping split to chapter: {e}")
            return None
    
    def _build_proper_chapter_mapping(self, kepub_zip: zipfile.ZipFile, opf_path: str, spine_order: List[str], manifest: Dict[str, str]) -> Dict[str, int]:
        """Build proper chapter mapping using EPUB NCX/TOC structure."""
        try:
            # Get EPUB structure to use its NCX/TOC
            # We'll look for the corresponding EPUB file and use its TOC
            proper_mapping = {}
            
            # First, try to find NCX file in the KEPUB
            ncx_path = None
            for file_path in kepub_zip.namelist():
                if file_path.endswith('.ncx') or 'toc.ncx' in file_path.lower():
                    ncx_path = file_path
                    break
            
            if ncx_path:
                log.debug(f"Found NCX file in KEPUB: {ncx_path}")
                ncx_content = kepub_zip.read(ncx_path).decode('utf-8')
                chapter_files = self._parse_ncx_for_chapters(ncx_content, opf_path)
                
                # Map content files to chapters based on NCX structure
                chapter_index = 0
                for spine_item in spine_order:
                    if spine_item not in manifest:
                        continue
                    
                    content_file = manifest[spine_item]
                    file_paths = [
                        content_file,
                        os.path.join(os.path.dirname(opf_path), content_file)
                    ]
                    
                    for file_path in file_paths:
                        # Check if this file corresponds to a chapter in the NCX
                        base_filename = os.path.basename(file_path)
                        
                        # Skip non-content files (title page, etc.)
                        if any(skip in base_filename.lower() for skip in ['title', 'cover', 'toc', 'copyright']):
                            proper_mapping[file_path] = -1  # Mark as non-chapter
                            continue
                        
                        # Map content files to chapters
                        # Look for matching files in NCX or use sequential mapping for content files
                        found_in_ncx = False
                        for ncx_file, ncx_chapter in chapter_files.items():
                            if base_filename in ncx_file or os.path.basename(ncx_file) == base_filename:
                                proper_mapping[file_path] = ncx_chapter
                                found_in_ncx = True
                                log.debug(f"NCX mapping: {base_filename} → chapter {ncx_chapter}")
                                break
                        
                        if not found_in_ncx:
                            # Sequential mapping for content files not explicitly in NCX
                            if 'split' in base_filename.lower() or 'chapter' in base_filename.lower():
                                proper_mapping[file_path] = chapter_index
                                chapter_index += 1
                                log.debug(f"Sequential mapping: {base_filename} → chapter {chapter_index - 1}")
                            else:
                                proper_mapping[file_path] = -1  # Non-chapter content
                        
                        break  # Only process the first valid path
            else:
                log.warning("No NCX file found in KEPUB, using sequential mapping")
                # Fallback: sequential mapping for content files
                chapter_index = 0
                for spine_item in spine_order:
                    if spine_item not in manifest:
                        continue
                    
                    content_file = manifest[spine_item]
                    file_paths = [
                        content_file,
                        os.path.join(os.path.dirname(opf_path), content_file)
                    ]
                    
                    for file_path in file_paths:
                        base_filename = os.path.basename(file_path)
                        
                        # Skip non-content files
                        if any(skip in base_filename.lower() for skip in ['title', 'cover', 'toc', 'copyright']):
                            proper_mapping[file_path] = -1
                            continue
                        
                        # Map content files sequentially
                        if 'split' in base_filename.lower() or 'chapter' in base_filename.lower():
                            proper_mapping[file_path] = chapter_index
                            chapter_index += 1
                        else:
                            proper_mapping[file_path] = -1
                        
                        break
            
            log.info(f"Built proper chapter mapping for {len(proper_mapping)} files")
            return proper_mapping
            
        except Exception as e:
            log.error(f"Error building proper chapter mapping: {e}")
            return {}
    
    def _parse_ncx_for_chapters(self, ncx_content: str, opf_path: str) -> Dict[str, int]:
        """Parse NCX content to extract chapter file mappings."""
        try:
            chapter_files = {}
            
            if HAS_BEAUTIFULSOUP:
                soup = BeautifulSoup(ncx_content, 'xml' if 'xml' in str(HAS_BEAUTIFULSOUP) else 'html.parser')
                nav_points = soup.find_all('navPoint')
                
                for i, nav_point in enumerate(nav_points):
                    content_tag = nav_point.find('content')
                    if content_tag and content_tag.get('src'):
                        src = content_tag.get('src')
                        # Remove fragment identifier
                        if '#' in src:
                            src = src.split('#')[0]
                        
                        # Resolve relative path
                        if not src.startswith('/'):
                            src = os.path.join(os.path.dirname(opf_path), src)
                        
                        chapter_files[src] = i
                        log.debug(f"NCX chapter {i}: {src}")
            else:
                # Fallback regex parsing
                import re
                content_pattern = r'<content\s+src="([^"]+)"'
                matches = re.findall(content_pattern, ncx_content, re.IGNORECASE)
                
                for i, src in enumerate(matches):
                    if '#' in src:
                        src = src.split('#')[0]
                    
                    if not src.startswith('/'):
                        src = os.path.join(os.path.dirname(opf_path), src)
                    
                    chapter_files[src] = i
            
            return chapter_files
            
        except Exception as e:
            log.error(f"Error parsing NCX for chapters: {e}")
            return {}
    
    def _extract_text_from_kobo_span_content_only(self, content: str, span_id: str, file_path: str) -> Optional[Dict]:
        """Extract text from a kobo.X.Y span for pure content matching - no chapter assumptions."""
        try:
            if HAS_BEAUTIFULSOUP:
                soup = BeautifulSoup(content, 'html.parser')
                span_element = soup.find('span', id=span_id)
                if span_element:
                    # Extract text from this span
                    span_text = span_element.get_text().strip()
                    
                    # Get much larger surrounding context for better matching
                    # Strategy: get several sentences before and after the span for unique matching
                    parent = span_element.parent
                    extended_context = ""
                    
                    if parent:
                        # Go up to find a larger container (paragraph, section, or body)
                        context_element = parent
                        while context_element and context_element.name not in ['p', 'div', 'section', 'body']:
                            context_element = context_element.parent
                        
                        if context_element:
                            # Get full paragraph/section text
                            full_text = context_element.get_text()
                            
                            # Find position of our span text within the full text
                            span_pos = full_text.find(span_text)
                            if span_pos >= 0:
                                # Extract larger context: 300 chars before + span + 300 chars after
                                start_pos = max(0, span_pos - 300)
                                end_pos = min(len(full_text), span_pos + len(span_text) + 300)
                                extended_context = full_text[start_pos:end_pos].strip()
                            else:
                                extended_context = full_text[:800]  # Fallback to first 800 chars
                        else:
                            extended_context = parent.get_text()[:800]
                    
                    if not extended_context or len(extended_context) < 100:
                        extended_context = span_text
                    
                    # Create a more unique search text by combining span + context
                    unique_search_text = span_text
                    if len(extended_context) > len(span_text) + 50:
                        # Use a portion of the extended context that includes our span
                        unique_search_text = extended_context
                    
                    log.info(f"Extracted span text: '{span_text[:100]}...'")
                    log.info(f"Extended context ({len(extended_context)} chars): '{extended_context[:200]}...'")
                    log.info(f"Search text for EPUB matching ({len(unique_search_text)} chars): '{unique_search_text[:150]}...'")
                    
                    # Return in the format expected by extract_text_from_kepub
                    return {
                        'text': unique_search_text,  # Use the more unique text for searching
                        'file': file_path,
                        'position': 0,  # Position within file is irrelevant for content matching
                        'context': extended_context,
                        'text_sample': span_text,  # Original span text
                        'surrounding_context': extended_context,
                        'original_span_text': span_text  # Keep original for comparison
                    }
            else:
                # Fallback regex approach
                span_pattern = rf'<span[^>]*id="{re.escape(span_id)}"[^>]*>(.*?)</span>'
                match = re.search(span_pattern, content, re.DOTALL | re.IGNORECASE)
                if match:
                    span_text = re.sub(r'<[^>]+>', '', match.group(1)).strip()
                    
                    # Try to get some context around the span
                    span_start = match.start()
                    context_start = max(0, span_start - 500)
                    context_end = min(len(content), match.end() + 500)
                    context_text = content[context_start:context_end]
                    context_text = re.sub(r'<[^>]+>', '', context_text).strip()
                    
                    return {
                        'text': span_text,
                        'file': file_path,
                        'position': 0,
                        'context': context_text,
                        'text_sample': span_text,
                        'surrounding_context': context_text
                    }
            
            return None
            
        except Exception as e:
            log.error(f"Error extracting text from span {span_id}: {e}")
            return None
    
    def _build_part_chapter_mapping(self, epub_structure) -> Dict[int, Dict[str, any]]:
        """Build robust mapping from EPUB chapter numbers to Part/Chapter structure."""
        mapping = {}
        
        if not epub_structure or not epub_structure.chapters:
            log.warning("No chapters available for Part/Chapter mapping")
            return mapping
        
        log.info(f"Analyzing {len(epub_structure.chapters)} chapters for Part/Chapter structure")
        
        current_part = 1
        chapter_in_part = 1
        
        # First pass: identify structure patterns
        chapter_titles = [chapter_info.title for chapter_info in epub_structure.chapters]
        has_explicit_parts = self._detect_part_structure(chapter_titles)
        
        for epub_chapter_index, chapter_info in enumerate(epub_structure.chapters):
            title = chapter_info.title.strip()
            href = chapter_info.file_path
            
            log.debug(f"TOC item {epub_chapter_index}: '{title}' -> {href}")
            
            # Check if this is a Part heading
            if self._is_part_header(title):
                part_number = self._extract_part_number(title)
                if part_number:
                    current_part = part_number
                else:
                    current_part += 1  # Auto-increment if we can't parse
                
                chapter_in_part = 1  # Reset chapter counter for new part
                log.info(f"Found Part {current_part}: '{title}'")
                
                # Map this TOC entry as a part header
                mapping[epub_chapter_index] = {
                    'part': current_part,
                    'chapter': 0,  # Part header, not a chapter
                    'title': title,
                    'is_part_header': True
                }
                continue
            
            # Check if this is a Chapter
            chapter_number = self._extract_chapter_number(title)
            
            if chapter_number is not None:
                # Explicit chapter number found
                if not has_explicit_parts and chapter_number == 1 and chapter_in_part > 1:
                    # Chapter numbering reset - likely indicates new part
                    current_part += 1
                    chapter_in_part = 1
                    log.info(f"Detected part transition at chapter reset to '{title}' -> Part {current_part}")
                else:
                    chapter_in_part = chapter_number
                
                log.info(f"Found Part {current_part} Chapter {chapter_in_part}: '{title}'")
            else:
                # No explicit chapter number - treat as sequential content
                if not any(skip in title.lower() for skip in ['cover', 'title', 'copyright', 'contents', 'table', 'toc', 'preface', 'foreword']):
                    log.info(f"Found sequential content in Part {current_part} Chapter {chapter_in_part}: '{title}'")
                else:
                    # Skip front/back matter
                    mapping[epub_chapter_index] = {
                        'part': 0,
                        'chapter': 0,
                        'title': title,
                        'is_part_header': False,
                        'is_front_matter': True
                    }
                    continue
            
            mapping[epub_chapter_index] = {
                'part': current_part,
                'chapter': chapter_in_part,
                'title': title,
                'is_part_header': False
            }
            
            # Increment for next iteration (only if no explicit numbering)
            if chapter_number is None:
                chapter_in_part += 1
        
        log.info(f"Built Part/Chapter mapping: {len(mapping)} entries")
        for epub_idx, info in mapping.items():
            if info.get('is_front_matter'):
                log.debug(f"EPUB chapter {epub_idx} = Front matter: {info['title']}")
            elif info['is_part_header']:
                log.debug(f"EPUB chapter {epub_idx} = Part {info['part']} header")
            else:
                log.debug(f"EPUB chapter {epub_idx} = Part {info['part']} Chapter {info['chapter']}")
        
        return mapping
    
    def _detect_part_structure(self, chapter_titles: List[str]) -> bool:
        """Detect if the book has explicit part structure."""
        part_indicators = ['part', 'partie', 'deel', 'teil', 'book', 'livre', 'boek', 'buch']
        
        for title in chapter_titles:
            title_lower = title.lower().strip()
            if any(indicator in title_lower for indicator in part_indicators):
                # Additional check: make sure it's actually a part header, not just containing the word
                if any(title_lower.startswith(indicator) for indicator in part_indicators):
                    return True
        
        return False
    
    def _is_part_header(self, title: str) -> bool:
        """Check if a title represents a part header."""
        title_lower = title.lower().strip()
        
        # Simple and specific check for common part patterns
        if title_lower == 'part one' or title_lower == 'part two' or title_lower == 'part three':
            return True
        if title_lower == 'part 1' or title_lower == 'part 2' or title_lower == 'part 3':
            return True
        if title_lower.startswith('part ') and len(title_lower.split()) == 2:
            return True
        
        return False
    
    def _extract_part_number(self, title: str) -> Optional[int]:
        """Extract part number from title."""
        title_lower = title.lower().strip()
        
        # Simple mapping for common cases
        if title_lower == 'part one':
            return 1
        elif title_lower == 'part two':
            return 2
        elif title_lower == 'part three':
            return 3
        elif title_lower == 'part 1':
            return 1
        elif title_lower == 'part 2':
            return 2
        elif title_lower == 'part 3':
            return 3
        
        return None
    
    def _extract_chapter_number(self, title: str) -> Optional[int]:
        """Extract chapter number from title - focus on roman numerals for The Stranger."""
        title_stripped = title.strip()
        
        # Simple roman numeral mapping for The Stranger
        roman_numerals = {
            'i': 1, 'ii': 2, 'iii': 3, 'iv': 4, 'v': 5, 'vi': 6, 'vii': 7, 'viii': 8, 'ix': 9, 'x': 10
        }
        
        # Check if title is just a roman numeral (like "I", "II", "III", etc.)
        if title_stripped.lower() in roman_numerals:
            return roman_numerals[title_stripped.lower()]
        
        # Check if title is just a number
        if title_stripped.isdigit():
            return int(title_stripped)
        
        return None
    
    def _roman_to_int(self, roman: str) -> Optional[int]:
        """Convert roman numeral to integer."""
        roman_values = {
            'i': 1, 'v': 5, 'x': 10, 'l': 50, 'c': 100, 'd': 500, 'm': 1000
        }
        
        roman = roman.lower()
        total = 0
        prev_value = 0
        
        try:
            for char in reversed(roman):
                if char not in roman_values:
                    return None
                
                value = roman_values[char]
                if value < prev_value:
                    total -= value
                else:
                    total += value
                prev_value = value
            
            return total if total > 0 else None
        except:
            return None
    
    def _enhance_position_with_part_chapter(self, epub_position, epub_path: str):
        """Enhance position information with Part/Chapter details."""
        try:
            # Get EPUB structure and build Part/Chapter mapping
            epub_structure = self.analyze_epub_structure(epub_path)
            if not epub_structure:
                return None
            
            part_chapter_mapping = self._build_part_chapter_mapping(epub_structure)
            
            if epub_position.chapter in part_chapter_mapping:
                part_chapter_info = part_chapter_mapping[epub_position.chapter]
                
                if part_chapter_info['is_part_header']:
                    log.info(f"Content-based mapping: EPUB chapter {epub_position.chapter} → Part {part_chapter_info['part']} header")
                    # For part headers, return with chapter 0
                    enhanced_position = PositionInfo(
                        chapter=0,  # Part header
                        paragraph=epub_position.paragraph,
                        character_offset=epub_position.character_offset,
                        chapter_progress=epub_position.chapter_progress,
                        part=part_chapter_info['part'],
                        chapter_in_part=0,
                        is_part_header=True
                    )
                else:
                    log.info(f"Content-based mapping: EPUB chapter {epub_position.chapter} → Part {part_chapter_info['part']} Chapter {part_chapter_info['chapter']}, progress {epub_position.chapter_progress:.1f}%")
                    enhanced_position = PositionInfo(
                        chapter=part_chapter_info['chapter'],  # Chapter within part
                        paragraph=epub_position.paragraph,
                        character_offset=epub_position.character_offset,
                        chapter_progress=epub_position.chapter_progress,
                        part=part_chapter_info['part'],
                        chapter_in_part=part_chapter_info['chapter'],
                        is_part_header=False
                    )
                
                return enhanced_position
            else:
                log.warning(f"EPUB chapter {epub_position.chapter} not found in Part/Chapter mapping")
                return None
                
        except Exception as e:
            log.error(f"Error enhancing position with Part/Chapter info: {e}")
            return None
    
    def _scan_kobo_spans_in_content(self, content: str, file_path: str) -> List[str]:
        """Scan content specifically for kobo.X.Y span IDs."""
        try:
            kobo_spans = []
            
            if HAS_BEAUTIFULSOUP:
                # Use BeautifulSoup for more accurate parsing
                soup = BeautifulSoup(content, 'html.parser')
                # Look for spans with IDs that match kobo.X.Y pattern
                all_spans = soup.find_all('span', id=True)
                for span in all_spans:
                    span_id = span.get('id')
                    if span_id and re.match(r'^kobo\.\d+\.\d+$', span_id):
                        kobo_spans.append(span_id)
            else:
                # Fallback: regex-based span finding
                kobo_span_pattern = r'<span[^>]*id="(kobo\.\d+\.\d+)"[^>]*>'
                kobo_spans = re.findall(kobo_span_pattern, content, re.IGNORECASE)
            
            if kobo_spans:
                log.debug(f"Found {len(kobo_spans)} kobo.X.Y spans in {os.path.basename(file_path)}: {kobo_spans[:5]}{'...' if len(kobo_spans) > 5 else ''}")
            
            return kobo_spans
            
        except Exception as e:
            log.debug(f"Error scanning kobo spans in {file_path}: {e}")
            return []
    
    def _extract_text_from_kobo_span(self, content: str, span_id: str, file_path: str, chapter_index: int) -> Optional[Dict]:
        """Extract text from a kobo.X.Y span and determine chapter information."""
        try:
            if HAS_BEAUTIFULSOUP:
                soup = BeautifulSoup(content, 'html.parser')
                span_element = soup.find('span', id=span_id)
                if span_element:
                    # Extract text from this span
                    span_text = span_element.get_text().strip()
                    
                    # Get surrounding context
                    parent = span_element.parent
                    if parent:
                        context_text = parent.get_text()[:500]  # First 500 chars for context
                    else:
                        context_text = span_text
                    
                    # Calculate approximate position within the chapter/file
                    all_spans_in_file = self._scan_kobo_spans_in_content(content, file_path)
                    if span_id in all_spans_in_file:
                        span_position = all_spans_in_file.index(span_id)
                        total_spans = len(all_spans_in_file)
                        chapter_progress = (span_position / total_spans) * 100 if total_spans > 0 else 0.0
                    else:
                        chapter_progress = 0.0
                    
                    log.info(f"Extracted text from {span_id}: '{span_text[:50]}...' (chapter {chapter_index}, progress {chapter_progress:.1f}%)")
                    
                    return {
                        'chapter': chapter_index,
                        'paragraph': span_position if 'span_position' in locals() else 0,
                        'character_offset': 0,
                        'chapter_progress': chapter_progress,
                        'text_sample': span_text[:100],
                        'context': context_text[:200],
                        'file_path': file_path,
                        # Also provide the format expected by extract_text_from_kepub
                        'text': span_text[:100],
                        'file': file_path,
                        'position': span_position if 'span_position' in locals() else 0
                    }
            else:
                # Fallback regex approach
                span_pattern = rf'<span[^>]*id="{re.escape(span_id)}"[^>]*>(.*?)</span>'
                match = re.search(span_pattern, content, re.DOTALL | re.IGNORECASE)
                if match:
                    span_text = re.sub(r'<[^>]+>', '', match.group(1)).strip()
                    
                    return {
                        'chapter': chapter_index,
                        'paragraph': 0,
                        'character_offset': 0,
                        'chapter_progress': 10.0,  # Rough estimate
                        'text_sample': span_text[:100],
                        'context': span_text[:200],
                        'file_path': file_path,
                        # Also provide the format expected by extract_text_from_kepub
                        'text': span_text[:100],
                        'file': file_path,
                        'position': 0
                    }
            
            return None
            
        except Exception as e:
            log.error(f"Error extracting text from span {span_id}: {e}")
            return None
    
    def _try_kobo_span_fallback(self, target_span_id: str, span_map: Dict) -> Optional[Dict]:
        """Try to find nearby kobo.X.Y spans if exact match fails."""
        try:
            # Parse target span ID
            match = re.match(r'^kobo\.(\d+)\.(\d+)$', target_span_id)
            if not match:
                return None
            
            target_x = int(match.group(1))
            target_y = int(match.group(2))
            
            log.info(f"Looking for nearby spans around {target_span_id} (X={target_x}, Y={target_y})")
            
            # Look for spans with same X but different Y
            nearby_candidates = []
            for span_id in span_map.keys():
                span_match = re.match(r'^kobo\.(\d+)\.(\d+)$', span_id)
                if span_match:
                    span_x = int(span_match.group(1))
                    span_y = int(span_match.group(2))
                    
                    # Same X value (same sequence), different Y
                    if span_x == target_x and abs(span_y - target_y) <= 5:
                        nearby_candidates.append((span_id, abs(span_y - target_y)))
                    # Similar X value
                    elif abs(span_x - target_x) <= 2:
                        nearby_candidates.append((span_id, abs(span_x - target_x) + abs(span_y - target_y)))
            
            # Sort by distance and try closest matches
            nearby_candidates.sort(key=lambda x: x[1])
            
            if nearby_candidates:
                log.info(f"Found {len(nearby_candidates)} nearby spans")
                for span_id, distance in nearby_candidates[:3]:  # Try top 3
                    log.info(f"Trying nearby span: {span_id} (distance: {distance})")
                    span_info = span_map[span_id]
                    result = self._extract_text_from_kobo_span_content_only(
                        span_info['content'], 
                        span_id, 
                        span_info['file_path']
                    )
                    if result:
                        log.info(f"Successfully found content using nearby span {span_id}")
                        return result
            
            return None
            
        except Exception as e:
            log.debug(f"Error trying kobo span fallback: {e}")
            return None
    
    def _scan_spans_in_content(self, content: str, file_path: str) -> List[str]:
        """Scan content for all span IDs to understand span structure."""
        try:
            spans = []
            all_span_ids = []
            
            if HAS_BEAUTIFULSOUP:
                # Use BeautifulSoup for more accurate parsing
                soup = BeautifulSoup(content, 'html.parser')
                
                # First, look for koboSpan specifically
                kobo_span_elements = soup.find_all('span', id=lambda x: x and x.startswith('koboSpan'))
                spans = [span.get('id') for span in kobo_span_elements if span.get('id')]
                
                # Also scan ALL span IDs to see what's actually there
                all_spans = soup.find_all('span', id=True)
                all_span_ids = [span.get('id') for span in all_spans if span.get('id')]
            else:
                # Fallback: regex-based span finding
                kobo_span_pattern = r'<span[^>]*id="(koboSpan\d+)"[^>]*>'
                spans = re.findall(kobo_span_pattern, content, re.IGNORECASE)
                
                # Also scan for ANY span IDs
                all_span_pattern = r'<span[^>]*id="([^"]+)"[^>]*>'
                all_span_ids = re.findall(all_span_pattern, content, re.IGNORECASE)
            
            # Debug output
            if spans:
                log.debug(f"Found {len(spans)} koboSpan elements in {os.path.basename(file_path)}: {spans[:5]}{'...' if len(spans) > 5 else ''}")
            elif all_span_ids:
                log.info(f"No koboSpan elements in {os.path.basename(file_path)}, but found {len(all_span_ids)} other spans: {all_span_ids[:10]}{'...' if len(all_span_ids) > 10 else ''}")
            else:
                log.debug(f"No span elements found in {os.path.basename(file_path)}")
            
            return spans
            
        except Exception as e:
            log.debug(f"Error scanning spans in {file_path}: {e}")
            return []
    
    def _try_nearby_spans(self, kepub_zip: zipfile.ZipFile, target_span_id: str, offset: int, all_spans: List[str]) -> Optional[Dict]:
        """Try to find nearby spans if the exact span isn't found."""
        try:
            # Extract target span number
            target_match = re.match(r'koboSpan(\d+)', target_span_id)
            if not target_match:
                return None
            
            target_num = int(target_match.group(1))
            log.info(f"Looking for nearby spans around {target_span_id} (number {target_num})")
            
            # Check for common patterns
            nearby_candidates = []
            
            # Check if spans start from 0 instead of 1
            if target_num == 1:
                nearby_candidates.extend([f"koboSpan0", f"koboSpan2"])
            
            # Check for off-by-one errors
            nearby_candidates.extend([
                f"koboSpan{target_num - 1}",
                f"koboSpan{target_num + 1}"
            ])
            
            # Look for any spans in the same range
            for span in all_spans:
                span_match = re.match(r'koboSpan(\d+)', span)
                if span_match:
                    span_num = int(span_match.group(1))
                    if abs(span_num - target_num) <= 5:  # Within 5 spans
                        nearby_candidates.append(span)
            
            # Remove duplicates and filter to existing spans
            nearby_candidates = list(set(nearby_candidates))
            existing_nearby = [span for span in nearby_candidates if span in all_spans]
            
            if existing_nearby:
                log.info(f"Found nearby spans: {existing_nearby[:3]}")
                
                # Try the first available nearby span
                for nearby_span in existing_nearby[:3]:  # Try up to 3 nearby spans
                    log.info(f"Trying nearby span: {nearby_span}")
                    result = self._find_span_in_kepub_simple(kepub_zip, nearby_span, offset)
                    if result:
                        log.info(f"Successfully found content using nearby span {nearby_span}")
                        return result
            
            return None
            
        except Exception as e:
            log.debug(f"Error trying nearby spans: {e}")
            return None
    
    def _find_span_in_kepub_simple(self, kepub_zip: zipfile.ZipFile, span_id: str, offset: int) -> Optional[Dict]:
        """Simple span search without extensive debugging."""
        try:
            opf_path = self._find_opf_file(kepub_zip)
            if not opf_path:
                return None
            
            opf_content = kepub_zip.read(opf_path).decode('utf-8')
            spine_order, manifest = self._parse_opf(opf_content)
            
            for spine_item in spine_order:
                if spine_item not in manifest:
                    continue
                
                content_file = manifest[spine_item]
                
                try:
                    content_paths = [
                        content_file,
                        os.path.join(os.path.dirname(opf_path), content_file)
                    ]
                    
                    content = None
                    actual_path = None
                    for content_path in content_paths:
                        try:
                            content = kepub_zip.read(content_path).decode('utf-8')
                            actual_path = content_path
                            break
                        except KeyError:
                            continue
                    
                    if not content:
                        continue
                    
                    span_info = self._extract_text_from_span(content, span_id, offset, actual_path)
                    if span_info:
                        return span_info
                
                except Exception as e:
                    continue
            
            return None
            
        except Exception as e:
            return None
    
    def _extract_text_from_span(self, html_content: str, span_id: str, offset: int, file_path: str) -> Optional[Dict]:
        """Extract text from a specific span ID in HTML content."""
        try:
            if not HAS_BEAUTIFULSOUP:
                # Fallback: regex-based span finding
                span_pattern = rf'<span[^>]*id="{re.escape(span_id)}"[^>]*>(.*?)</span>'
                match = re.search(span_pattern, html_content, re.DOTALL | re.IGNORECASE)
                if match:
                    span_text = re.sub(r'<[^>]+>', '', match.group(1))  # Strip inner HTML
                    span_text = span_text.strip()
                    
                    if len(span_text) > offset:
                        # Apply offset within the span
                        offset_text = span_text[offset:offset+100] if offset > 0 else span_text[:100]
                        context_text = span_text[:300] if len(span_text) > 300 else span_text
                        
                        return {
                            'text': offset_text.strip(),
                            'context': context_text.strip(),
                            'position': offset,
                            'file': file_path
                        }
                return None
            
            # Use BeautifulSoup for better HTML parsing
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the span with the specific ID
            span = soup.find('span', id=span_id)
            if not span:
                return None
            
            # Extract text from the span
            span_text = span.get_text()
            if not span_text:
                return None
            
            log.debug(f"Found span {span_id} with text: '{span_text[:50]}...'")
            
            # Apply offset within the span text
            if len(span_text) <= offset:
                # Offset is beyond span text, use entire span
                offset_text = span_text
                actual_offset = 0
            else:
                # Extract text starting from offset
                offset_text = span_text[offset:offset+100]  # 100 char sample
                actual_offset = offset
            
            # Get larger context around the span
            parent = span.parent
            context_text = parent.get_text() if parent else span_text
            context_text = context_text[:300] if len(context_text) > 300 else context_text
            
            return {
                'text': offset_text.strip(),
                'context': context_text.strip(),
                'position': actual_offset,
                'file': file_path
            }
        
        except Exception as e:
            log.error(f"Error extracting text from span {span_id}: {e}")
            return None
    
    def extract_text_from_kepub(self, kepub_path: str, kobo_location: str, progress_percent: float = None) -> Optional[TextSample]:
        """Extract text content from KEPUB file at the specified Kobo location using span ID search."""
        try:
            with zipfile.ZipFile(kepub_path, 'r') as kepub_zip:
                # Parse the kobo location to get span ID and offset
                span_id, offset = self._parse_kobo_span_location(kobo_location)
                
                if span_id is None:
                    log.debug(f"Could not parse Kobo span location: {kobo_location}")
                    return None
                
                log.debug(f"Searching for span ID: {span_id}, offset: {offset}")
                
                # Search for the span across all content files
                span_info = self._find_span_in_kepub(kepub_zip, span_id, offset, progress_percent)
                
                if not span_info:
                    log.debug(f"Could not find span {span_id} in KEPUB content")
                    return None
                
                log.debug(f"Found span {span_id} in file: {span_info['file']}")
                
                return TextSample(
                    text=span_info['text'],
                    chapter_file=span_info['file'],
                    position_in_file=span_info['position'],
                    surrounding_context=span_info['context']
                )
        
        except Exception as e:
            log.error(f"Error extracting text from KEPUB {kepub_path}: {e}")
            return None
        
        return None
    
    def search_text_in_epub(self, epub_path: str, target_text: str, context: str = "") -> Optional[PositionInfo]:
        """Search for specific text in EPUB file and return position information."""
        try:
            with zipfile.ZipFile(epub_path, 'r') as epub_zip:
                # Get EPUB structure
                epub_structure = self.analyze_epub_structure(epub_path)
                if not epub_structure:
                    return None
                
                # Get OPF and manifest info
                opf_path = self._find_opf_file(epub_zip)
                if not opf_path:
                    return None
                
                opf_content = epub_zip.read(opf_path).decode('utf-8')
                spine_order, manifest = self._parse_opf(opf_content)
                
                # Search through all content files
                for chapter_idx, spine_item in enumerate(spine_order):
                    if spine_item not in manifest:
                        continue
                    
                    content_file = manifest[spine_item]
                    
                    try:
                        # Try different path variations
                        content_paths = [
                            content_file,
                            os.path.join(os.path.dirname(opf_path), content_file)
                        ]
                        
                        content = None
                        for content_path in content_paths:
                            try:
                                content = epub_zip.read(content_path).decode('utf-8')
                                break
                            except KeyError:
                                continue
                        
                        if not content:
                            continue
                        
                        # Search for the target text in this file
                        match_info = self._search_text_in_html(content, target_text, context)
                        if match_info:
                            # Calculate chapter progress based on position within file
                            file_length = len(self._strip_html_tags(content))
                            chapter_progress = (match_info['position'] / file_length) * 100 if file_length > 0 else 0
                            
                            # Extract surrounding text from EPUB to compare with KEPUB
                            epub_plain_text = self._strip_html_tags(content)
                            match_start = match_info['position']
                            match_end = match_start + len(target_text)
                            
                            # Get context around the match in EPUB
                            context_start = max(0, match_start - 200)
                            context_end = min(len(epub_plain_text), match_end + 200)
                            epub_context = epub_plain_text[context_start:context_end]
                            
                            log.info(f"EPUB MATCH FOUND in chapter {chapter_idx} at position {match_start}")
                            log.info(f"EPUB matched text: '{epub_plain_text[match_start:match_end][:150]}...'")
                            log.info(f"EPUB context: '{epub_context[:200]}...'")
                            log.info(f"KEPUB search text was: '{target_text[:150]}...'")
                            log.info(f"Match confidence: chapter progress {chapter_progress:.1f}%")
                            
                            return PositionInfo(
                                chapter=chapter_idx,
                                paragraph=match_info['paragraph'],
                                character_offset=match_info['position'],
                                chapter_progress=min(chapter_progress, 100.0)
                            )
                    
                    except Exception as e:
                        log.debug(f"Error searching in file {content_file}: {e}")
                        continue
        
        except Exception as e:
            log.error(f"Error searching text in EPUB {epub_path}: {e}")
        
        return None
    
    def _extract_text_sample_from_html(self, html_content: str, target_position: int) -> Optional[Dict]:
        """Extract a text sample from HTML content at approximately the target position."""
        try:
            # Strip HTML to get plain text
            plain_text = self._strip_html_tags(html_content)
            
            if len(plain_text) == 0:
                return None
            
            # Calculate approximate position in the plain text
            # target_position might be a Kobo span ID or character offset
            if target_position > len(plain_text):
                # Use percentage-based position if number is too large
                position = min(int(len(plain_text) * 0.1), len(plain_text) - 1)
            else:
                position = min(target_position, len(plain_text) - 1)
            
            # Extract a sample of text around this position
            sample_length = 100  # Characters
            context_length = 300  # Larger context for matching
            
            start_pos = max(0, position - sample_length // 2)
            end_pos = min(len(plain_text), position + sample_length // 2)
            
            context_start = max(0, position - context_length // 2)
            context_end = min(len(plain_text), position + context_length // 2)
            
            sample_text = plain_text[start_pos:end_pos].strip()
            context_text = plain_text[context_start:context_end].strip()
            
            if len(sample_text) < 10:  # Too short to be useful
                return None
            
            return {
                'sample': sample_text,
                'context': context_text,
                'position': position
            }
        
        except Exception as e:
            log.error(f"Error extracting text sample: {e}")
            return None
    
    def _search_text_in_html(self, html_content: str, target_text: str, context: str = "") -> Optional[Dict]:
        """Search for target text in HTML content and return position information."""
        try:
            # Strip HTML to get plain text
            plain_text = self._strip_html_tags(html_content)
            
            # Try exact match first
            position = plain_text.find(target_text)
            
            # If no exact match, try fuzzy matching
            if position == -1 and len(target_text) > 20:
                # Use difflib for approximate matching
                words = plain_text.split()
                target_words = target_text.split()
                
                # Look for best matching sequence
                matcher = difflib.SequenceMatcher(None, words, target_words)
                match = matcher.find_longest_match(0, len(words), 0, len(target_words))
                
                if match.size > len(target_words) * 0.6:  # At least 60% match
                    # Reconstruct position from word match
                    matched_text = ' '.join(words[match.a:match.a + match.size])
                    position = plain_text.find(matched_text)
            
            if position == -1:
                return None
            
            # Calculate paragraph number (approximate)
            text_before = plain_text[:position]
            paragraph = text_before.count('\n\n') + text_before.count('\n \n') + 1
            
            return {
                'position': position,
                'paragraph': paragraph,
                'matched_text': target_text
            }
        
        except Exception as e:
            log.error(f"Error searching text in HTML: {e}")
            return None
    
    def _strip_html_tags(self, html_content: str) -> str:
        """Remove HTML tags and return plain text."""
        try:
            if not HAS_BEAUTIFULSOUP:
                # Fallback: simple regex-based tag removal
                clean_text = re.sub(r'<[^>]+>', '', html_content)
                clean_text = re.sub(r'\s+', ' ', clean_text)  # Normalize whitespace
                return clean_text.strip()
            
            # Use BeautifulSoup for better HTML parsing
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text content
            text = soup.get_text()
            
            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            return text
        
        except Exception as e:
            log.error(f"Error stripping HTML tags: {e}")
            return html_content  # Return original if parsing fails


# Global analyzer instance
book_analyzer = BookAnalyzer()