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
import json
import tempfile
from datetime import datetime, timezone

from . import logger, ub, gdriveutils
from .constants import CONFIG_DIR as _CONFIG_DIR

log = logger.create()

# Check if Google Drive support is available
try:
    if gdriveutils.gdrive_support:
        from pydrive2.auth import GoogleAuth
        from pydrive2.drive import GoogleDrive
        from pydrive2.auth import RefreshError
        from pydrive2.files import ApiRequestError
    else:
        try:
            from pydrive.auth import GoogleAuth
            from pydrive.drive import GoogleDrive
            from pydrive.auth import RefreshError
            from pydrive.files import ApiRequestError
        except ImportError:
            GoogleAuth = GoogleDrive = RefreshError = ApiRequestError = None
except ImportError:
    GoogleAuth = GoogleDrive = RefreshError = ApiRequestError = None

# Use same client secrets as main Google Drive integration
CLIENT_SECRETS = os.path.join(_CONFIG_DIR, 'client_secrets.json')

class UserGdriveAuth:
    """Per-user Google Drive authentication for Moonreader sync"""
    
    def __init__(self, user_id):
        self.user_id = user_id
        self.credentials_record = None
        self.drive = None
        self._load_user_credentials()
    
    def _load_user_credentials(self):
        """Load credentials from database for specific user"""
        try:
            self.credentials_record = ub.session.query(ub.UserGdriveCredentials).filter(
                ub.UserGdriveCredentials.user_id == self.user_id
            ).first()
        except Exception as e:
            log.error(f"Error loading user Google Drive credentials: {e}")
            
    def is_authenticated(self):
        """Check if user has valid Google Drive authentication"""
        return (self.credentials_record and 
                self.credentials_record.authenticated and 
                self.credentials_record.credentials_json)
    
    def get_authenticated_drive(self):
        """Get GoogleDrive instance for this user"""
        if not self.is_authenticated():
            return None
            
        if self.drive:
            return self.drive
            
        try:
            # Load client config from file
            import json
            with open(CLIENT_SECRETS, 'r') as f:
                client_config = json.load(f)
            
            # Create GoogleAuth with direct settings
            settings = {
                'client_config_backend': 'settings',
                'client_config': client_config['web'],
                'save_credentials': False,
                'oauth_scope': ['https://www.googleapis.com/auth/drive']
            }
            gauth = GoogleAuth(settings=settings)
            
            # Load user's credentials from database
            credentials_data = json.loads(self.credentials_record.credentials_json)
            
            # Create credentials object
            try:
                from oauth2client.client import OAuth2Credentials
                gauth.credentials = OAuth2Credentials.from_json(json.dumps(credentials_data))
            except ImportError:
                # Try google-auth-oauthlib for newer versions
                from google.oauth2.credentials import Credentials
                gauth.credentials = Credentials.from_authorized_user_info(credentials_data)
            
            # Check if credentials need refresh
            if gauth.access_token_expired:
                try:
                    gauth.Refresh()
                    # Update credentials in database
                    self.credentials_record.credentials_json = gauth.credentials.to_json()
                    self.credentials_record.last_refresh = datetime.now(timezone.utc)
                    ub.session.commit()
                except RefreshError as e:
                    log.error(f"Failed to refresh Google Drive credentials for user {self.user_id}: {e}")
                    # Mark as not authenticated if refresh fails
                    self.credentials_record.authenticated = False
                    ub.session.commit()
                    return None
            else:
                gauth.Authorize()
            
            # Create and cache drive instance
            self.drive = GoogleDrive(gauth)
                
            return self.drive
            
        except Exception as e:
            log.error(f"Error creating Google Drive instance for user {self.user_id}: {e}")
            return None
    
    def list_user_folders(self, parent_id='root'):
        """List folders in user's Google Drive under a specific parent"""
        drive = self.get_authenticated_drive()
        if not drive:
            return []
            
        try:
            folder_query = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            file_list = drive.ListFile({'q': folder_query}).GetList()
            return file_list
        except Exception as e:
            log.error(f"Error listing folders for user {self.user_id} under parent {parent_id}: {e}")
            return []
    
    def get_folder_by_path(self, path):
        """Get folder ID by path (like /Apps/Books/.Moon+/Cache)"""
        drive = self.get_authenticated_drive()
        if not drive:
            return None
            
        try:
            current_folder_id = 'root'
            path_parts = [p for p in path.split('/') if p]  # Remove empty parts
            
            for folder_name in path_parts:
                query = f"title = '{folder_name.replace(chr(39), chr(92)+chr(39))}' and '{current_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                folder_list = drive.ListFile({'q': query}).GetList()
                
                if not folder_list:
                    # Folder doesn't exist, create it
                    new_folder = drive.CreateFile({
                        'title': folder_name,
                        'parents': [{'kind': 'drive#fileLink', 'id': current_folder_id}],
                        'mimeType': 'application/vnd.google-apps.folder'
                    })
                    new_folder.Upload()
                    current_folder_id = new_folder['id']
                else:
                    current_folder_id = folder_list[0]['id']
            
            return current_folder_id
            
        except Exception as e:
            log.error(f"Error getting folder by path {path} for user {self.user_id}: {e}")
            return None
    
    def upload_file_to_folder(self, file_path, filename, folder_path):
        """Upload file to specific folder in user's Google Drive"""
        drive = self.get_authenticated_drive()
        if not drive:
            return False
            
        try:
            folder_id = self.get_folder_by_path(folder_path)
            if not folder_id:
                return False
                
            # Check if file already exists
            existing_files = drive.ListFile({
                'q': f"title = '{filename.replace(chr(39), chr(92)+chr(39))}' and '{folder_id}' in parents and trashed = false"
            }).GetList()
            
            if existing_files:
                # Update existing file
                drive_file = existing_files[0]
            else:
                # Create new file
                drive_file = drive.CreateFile({
                    'title': filename,
                    'parents': [{'kind': 'drive#fileLink', 'id': folder_id}]
                })
            
            drive_file.SetContentFile(file_path)
            drive_file.Upload()
            return True
            
        except Exception as e:
            log.error(f"Error uploading file {filename} for user {self.user_id}: {e}")
            return False
    
    def download_file_from_folder(self, filename, folder_path):
        """Download file from specific folder in user's Google Drive"""
        drive = self.get_authenticated_drive()
        if not drive:
            return None
            
        try:
            folder_id = self.get_folder_by_path(folder_path)
            if not folder_id:
                return None
                
            # Find the file
            file_list = drive.ListFile({
                'q': f"title = '{filename.replace(chr(39), chr(92)+chr(39))}' and '{folder_id}' in parents and trashed = false"
            }).GetList()
            
            if not file_list:
                return None
                
            drive_file = file_list[0]
            return drive_file
            
        except Exception as e:
            log.error(f"Error downloading file {filename} for user {self.user_id}: {e}")
            return None
    
    def list_files_in_folder(self, folder_path, pattern=None):
        """List files in user's Google Drive folder, optionally filtered by pattern."""
        drive = self.get_authenticated_drive()
        if not drive:
            return []
            
        try:
            folder_id = self.get_folder_by_path(folder_path)
            if not folder_id:
                return []
                
            # Query for files in the folder
            query = f"'{folder_id}' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
            
            # Add pattern matching if specified
            if pattern:
                # Convert simple pattern like "*.epub.po" to search term
                if pattern.startswith('*') and pattern.endswith('.epub.po'):
                    # For pattern like "*.epub.po", just search for files ending with .epub.po
                    query += " and title contains '.epub.po'"
                elif pattern:
                    # For other patterns, use contains search
                    search_term = pattern.replace('*', '').replace('.', '\\.')
                    query += f" and title contains '{search_term}'"
            
            file_list = drive.ListFile({'q': query}).GetList()
            return file_list
            
        except Exception as e:
            log.error(f"Error listing files in folder {folder_path} for user {self.user_id}: {e}")
            return []

    def get_user_email(self):
        """Get the Google account email for this user"""
        if self.credentials_record:
            return self.credentials_record.email
        return None
    
    def disconnect(self):
        """Remove user's Google Drive connection"""
        try:
            if self.credentials_record:
                ub.session.delete(self.credentials_record)
                ub.session.commit()
                self.credentials_record = None
                self.drive = None
                return True
        except Exception as e:
            log.error(f"Error disconnecting Google Drive for user {self.user_id}: {e}")
            ub.session.rollback()
        return False


def get_user_gdrive_status(user_id):
    """Get Google Drive status for a specific user"""
    if not gdriveutils.gdrive_support:
        return {
            'authenticated': False,
            'error': 'Google Drive not supported',
            'email': None
        }
    
    try:
        credentials = ub.session.query(ub.UserGdriveCredentials).filter(
            ub.UserGdriveCredentials.user_id == user_id
        ).first()
        
        if credentials and credentials.authenticated:
            return {
                'authenticated': True,
                'error': None,
                'email': credentials.email,
                'folder_name': credentials.folder_name
            }
        else:
            return {
                'authenticated': False,
                'error': None,
                'email': None
            }
    except Exception as e:
        log.error(f"Error getting Google Drive status for user {user_id}: {e}")
        return {
            'authenticated': False,
            'error': str(e),
            'email': None
        }