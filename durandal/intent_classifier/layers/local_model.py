import logging
import os
import glob
from pathlib import Path

import torch
import torch.nn as nn

from intent_classifier.conf import conf

from .base import IntentLayer


logger = logging.getLogger(conf.APP_NAME)



class LocalModelIntentLayer(IntentLayer):
    """
    Local PyTorch model-based intent classification layer.
    """
    
    def __init__(self, weights_path="models/local_model.pt", device="cpu", 
                 batch_size=2, confidence_threshold=0.75, **kwargs):
        super().__init__(**kwargs)
        self.weights_path = weights_path
        self.device = device
        self.batch_size = batch_size
        self.confidence_threshold = confidence_threshold
        self.model = None
        self.vocab = None  # Will be loaded with the model
        self.intent_labels = None  # Will be loaded with the model
        
        logger.debug(f"Initialized LocalModelIntentLayer with weights_path: {weights_path}, device: {device}")
    
    
    async def on_startup(self):
        """
        Initialize and load the PyTorch model during startup.
        """
        logger.debug("Starting LocalModelIntentLayer initialization...")
        
        try:
     
            if model_path is None:
                logger.warning(f"No model files found in {results_dir}. Creating default model...")
                # Create a default model for testing
                self._create_default_model()
                return
            
            # Load the model checkpoint
            logger.debug(f"Loading model from: {model_path}")
            checkpoint = torch.load(model_path, map_location=self.device)
            
            # Extract model parameters and metadata
            if isinstance(checkpoint, dict):
                model_state = checkpoint.get('model_state_dict', checkpoint.get('state_dict', checkpoint))
                self.vocab = checkpoint.get('vocab', self._create_default_vocab())
                self.intent_labels = checkpoint.get('intent_labels', self._create_default_labels())
                model_config = checkpoint.get('model_config', {})
            else:
                # Assume it's just the state dict
                model_state = checkpoint
                self.vocab = self._create_default_vocab()
                self.intent_labels = self._create_default_labels()
                model_config = {}
            
            # Initialize model with config or defaults
            vocab_size = model_config.get('vocab_size', len(self.vocab))
            embedding_dim = model_config.get('embedding_dim', 128)
            hidden_dim = model_config.get('hidden_dim', 256)
            num_classes = model_config.get('num_classes', len(self.intent_labels))
            
            logger.debug(f"Creating model with vocab_size={vocab_size}, num_classes={num_classes}")
            self.model = IntentClassificationModel(
                vocab_size=vocab_size,
                embedding_dim=embedding_dim,
                hidden_dim=hidden_dim,
                num_classes=num_classes
            )
            
            # Load the state dict
            self.model.load_state_dict(model_state)
            self.model.to(self.device)
            self.model.eval()
            
            logger.info(f"Successfully loaded LocalModelIntentLayer model from {model_path}")
            logger.debug(f"Model has {sum(p.numel() for p in self.model.parameters())} parameters")
            logger.debug(f"Vocabulary size: {len(self.vocab)}")
            logger.debug(f"Intent labels: {self.intent_labels}")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            logger.debug("Creating default model as fallback...")
            self._create_default_model()
    
    def _create_default_model(self):
        """Create a default model for testing when no trained model is available."""
        logger.debug("Creating default model for testing...")
        
        self.vocab = self._create_default_vocab()
        self.intent_labels = self._create_default_labels()
        
        self.model = IntentClassificationModel(
            vocab_size=len(self.vocab),
            embedding_dim=128,
            hidden_dim=256,
            num_classes=len(self.intent_labels)
        )
        self.model.to(self.device)
        self.model.eval()
        
        logger.warning("Using default untrained model. Results will be random.")
    
    def _create_default_vocab(self):
        """Create a basic vocabulary for testing."""
        return {
            '<PAD>': 0, '<UNK>': 1, '<START>': 2, '<END>': 3,
            'hello': 4, 'hi': 5, 'goodbye': 6, 'bye': 7, 'thanks': 8, 'thank': 9,
            'you': 10, 'please': 11, 'help': 12, 'what': 13, 'when': 14, 'where': 15,
            'how': 16, 'why': 17, 'who': 18, 'can': 19, 'could': 20, 'would': 21,
            'is': 22, 'are': 23, 'was': 24, 'were': 25, 'the': 26, 'a': 27, 'an': 28,
            'and': 29, 'or': 30, 'but': 31, 'in': 32, 'on': 33, 'at': 34, 'to': 35,
            'for': 36, 'with': 37, 'by': 38, 'from': 39, 'up': 40, 'down': 41,
        }
    
    def _create_default_labels(self):
        """Create default intent labels for testing."""
        return ['greeting', 'goodbye', 'question', 'request', 'thanks', 'complaint', 'compliment', 'unknown']
    
    def _tokenize_text(self, text: str, max_length: int = 32):
        """
        Simple tokenization for text input.
        In a real implementation, this would match your training preprocessing.
        """
        # Basic tokenization - split on spaces and convert to lowercase
        tokens = text.lower().strip().split()
        
        # Convert to token IDs
        token_ids = []
        for token in tokens[:max_length-2]:  # Leave room for START/END tokens
            token_ids.append(self.vocab.get(token, self.vocab['<UNK>']))
        
        # Add padding if needed
        while len(token_ids) < max_length:
            token_ids.append(self.vocab['<PAD>'])
        
        return torch.tensor(token_ids, dtype=torch.long).unsqueeze(0)  # Add batch dimension
    
    
    async def classify(self, previous_segments: list[str], segment: str, is_partial: bool = False) -> dict:
        """
        Classify the given text segment using the loaded PyTorch model.
        """
        logger.debug(f"Starting classification for segment: '{segment[:50]}...' (partial: {is_partial})")
        logger.debug(f"Previous segments count: {len(previous_segments)}")
        
        try:
            logger.debug("Tokenizing input text...")
            input_tensor = self._tokenize_text(segment)
            input_tensor = input_tensor.to(self.device)
            logger.debug(f"Input tensor shape: {input_tensor.shape}")
            
            # Run inference
            logger.debug("Running model inference...")
            with torch.no_grad():
                logits, probabilities = self.model(input_tensor)
            
            # Get predictions
            predicted_class = torch.argmax(probabilities, dim=-1).item()
            confidence = probabilities[0, predicted_class].item()
            
            intent_label = self.intent_labels[predicted_class]
            
            logger.debug(f"Model predictions - Intent: {intent_label}, Confidence: {confidence:.4f}")
            logger.debug(f"Full probability distribution: {probabilities[0].tolist()}")
            
            # Create result dictionary
            result = {
                "intent": intent_label,
                "confidence": float(confidence),
                "raw_text": segment,
                "is_partial": is_partial,
                "model_info": {
                    "predicted_class": int(predicted_class),
                    "all_probabilities": {
                        label: float(prob) for label, prob in zip(self.intent_labels, probabilities[0])
                    }
                }
            }
            
            logger.debug(f"Classification completed successfully: {result['intent']} (confidence: {result['confidence']:.4f})")
            return result
            
        except Exception as e:
            logger.error(f"Error during classification: {e}", exc_info=True)
            return {
                "intent": "unknown",
                "confidence": 0.0,
                "error": str(e),
                "raw_text": segment,
                "is_partial": is_partial
            }
