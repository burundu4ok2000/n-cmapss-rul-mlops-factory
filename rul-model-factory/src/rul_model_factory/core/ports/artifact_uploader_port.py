# Copyright 2026 Stanislav Burundukov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DOMAIN: LOGISTICS
COMPONENT: PORTS
SUBCOMPONENT: ARTIFACT_UPLOADER_PORT

VERSION: 0.1.0

ROLE:
    Defines the abstract interface for transporting validated assets (models, 
    parquets, logs) from the local compute environment to a persistent 
    industrial data lake (GCS, AWS S3, or On-Premise storage).

REGULATORY ALIGNMENT:
    - [DORA Compliance]: Ensures reliable off-site storage of audit trails.
    - [EU AI Act Article 12]: Facilitates long-term storage of technical documentation.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

class ArtifactUploaderPort(ABC):
    """
    Interface for the industrial artifact courier.
    Isolates the Core Domain from specific cloud provider SDKs (google-cloud-storage, etc.).
    """

    @abstractmethod
    def upload_artifact(
        self, 
        local_path: Path, 
        remote_path: str,
        content_type: Optional[str] = None
    ) -> str:
        """
        Transports a single validated file to the remote storage.
        
        Args:
            local_path: Absolute path to the local file.
            remote_path: Target path/key in the remote storage.
            content_type: Optional MIME type for the artifact.
            
        Returns:
            The full URI of the uploaded artifact (e.g., 'gs://bucket/path/file.ext').
        """
        pass

    @abstractmethod
    def upload_directory(
        self, 
        local_dir: Path, 
        remote_prefix: str,
        recursive: bool = True
    ) -> List[str]:
        """
        Transports a collection of artifacts to the remote storage.
        
        Args:
            local_dir: Absolute path to the source directory.
            remote_prefix: Destination prefix in the remote storage.
            recursive: Whether to include subdirectories.
            
        Returns:
            A list of URIs for all successfully uploaded artifacts.
        """
        pass

    @abstractmethod
    def verify_destination_readiness(self) -> bool:
        """
        Verifies that the remote storage (bucket/container) is accessible 
        and writable under the current security context.
        """
        pass

    @property
    @abstractmethod
    def base_uri(self) -> str:
        """
        The root URI of the storage destination (e.g., 'gs://my-factory-bucket').
        """
        pass
