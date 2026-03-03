from PIL import Image
from io import BytesIO
import imagehash
import numpy as np
import requests

def format_images(images):
        
    results = []
    num_images = len(images)
    
    if num_images == 0:
        return images
    
    #expect no more than 2, if more ignore
    num_images = 2 if num_images > 2 else num_images
    
    if num_images == 2:
       
       image1 = images[0]
       image2 = images[1]
           
       hash1 = imagehash.phash(image1)  # perceptual hash (best for cover matching)
       hash2 = imagehash.phash(image2)

       # Hamming distance
       distance = hash1 - hash2
       unique_count = 1 if distance < 30 else 2
       if unique_count == 1:
           w1, h1 = image1.size
           w2, h2 = image2.size
           selected_image = image1 if h1*w1 > h2*w2 else image2
           images[0] = selected_image 
    else:
       unique_count = 1

    for i in range(0, unique_count):
        image = images[i]  
        image = crop_to_book_aspect(image)
        image = resize_to_book_thumbnail(image)
        results.append(image)
 
    return results

def is_image_unique(url, image_candidates):                
                
    new_image = load_image_from_url(url)
    if is_mostly_grayscale(new_image): #unlikey a proper image
        return None, None
    new_image = crop_to_book_aspect(new_image) #cut to likely book size
    new_hashtag = imagehash.phash(new_image)
    if len(image_candidates) == 0:
        return new_hashtag, new_image
    for hashtag, _ in image_candidates.values():
        distance = hashtag - new_hashtag
        if distance < 30: #not unique
            return None, None 
    else:
        return new_hashtag, new_image


def resize_to_book_thumbnail(image: Image.Image, width: int = 160, height: int = 240) -> Image.Image:
    """
    Resize a 2:3 aspect ratio image to exactly 160x240 pixels (or custom size).
    Assumes input image already has the correct aspect ratio.
    """
    return image.resize((width, height), Image.LANCZOS)  # LANCZOS = high-quality downsampling


def load_image_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors

        img = Image.open(BytesIO(response.content))
        return img

    except Exception as e:
        print(f"Failed to load image: {e}")
        return None



def save_image_from_url(url, output_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise error if download failed

        with open(output_path, 'wb') as out_file:
            for chunk in response.iter_content(1024):
                out_file.write(chunk)

        print(f"Image saved to {output_path}")
        return True

    except Exception as e:
        print(f"Failed to save image: {e}")
        return False


def is_mostly_grayscale(image: Image.Image, tolerance=5, threshold=0.90):
    rgb = image.convert('RGB')
    data = np.array(rgb).reshape(-1, 3)
    diffs = np.abs(data[:, 0] - data[:, 1]) + \
            np.abs(data[:, 1] - data[:, 2]) + \
            np.abs(data[:, 0] - data[:, 2])
    grayscale_pixels = np.sum(diffs <= 3 * tolerance)
    return grayscale_pixels / len(data) >= threshold


def crop_to_book_aspect(image: Image.Image) -> Image.Image:
    """
    Crop a PIL image to a 2:3 aspect ratio (width:height = 2:3) centered.
    If the image's aspect ratio is wider than 2:3, it will crop width.
    If narrower, it will crop height.
    """
    target_ratio = 2 / 3
    w, h = image.size
    current_ratio = w / h

    if current_ratio > target_ratio:
        # Too wide → crop width
        new_w = int(h * target_ratio)
        left = (w - new_w) // 2
        right = left + new_w
        top = 0
        bottom = h
    elif current_ratio < target_ratio:
        # Too tall → crop height
        new_h = int(w / target_ratio)
        top = (h - new_h) // 2
        bottom = top + new_h
        left = 0
        right = w
    else:
        # Already the correct ratio
        return image

    return image.crop((left, top, right, bottom))
