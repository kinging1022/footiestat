from django.db import models

# Create your models here.
from django.utils import timezone
from django.utils.text import slugify
from django.contrib.auth.models import User


class Article(models.Model):
    ARTICLE_TYPES = [
        ('match_preview', 'Match Preview'),
        ('match_report', 'Match Report'),
        ('analysis', 'Tactical Analysis'),
        ('news', 'Breaking News'),
        ('interview', 'Interview'),
        ('opinion', 'Opinion Piece'),
        ('feature', 'Feature Story'),

    ]
    # Core fields
    title = models.CharField(max_length=200)
    slug = models.SlugField(unique=True, max_length=250, blank=True)
    article_type = models.CharField(max_length=20, choices=ARTICLE_TYPES, default='news')
    
    # Images - support up to 2 images
    image_1 = models.ImageField(upload_to='articles/', blank=True, null=True, help_text="Primary image")
    image_2 = models.ImageField(upload_to='articles/', blank=True, null=True, help_text="Secondary image (optional)")
    
    # Content
    excerpt = models.TextField(max_length=300, help_text="Brief summary for previews")
    content = models.TextField(help_text="Full article content (supports HTML)")
    
    # Metadata
    author = models.ForeignKey(
        User, 
        on_delete=models.SET_NULL, 
        null=True,
        related_name='articles'
    )
    published_date = models.DateTimeField(default=timezone.now)
    read_time = models.IntegerField(default=5, help_text="Reading time in minutes")
    tags = models.JSONField(default=list, blank=True)
    
    # Status
    is_published = models.BooleanField(default=True)
    is_featured = models.BooleanField(default=False)
    
    # Analytics
    view_count = models.PositiveIntegerField(default=0)
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-published_date']
        indexes = [
            models.Index(fields=['-published_date']),
            models.Index(fields=['-view_count']),
            models.Index(fields=['article_type']),
            models.Index(fields=['slug']),
            models.Index(fields=['is_published']),
        ]
    
    def __str__(self):
        return self.title
    

    def save(self, *args, **kwargs):
        if not self.slug:
            base_slug = slugify(self.title)
            slug = base_slug
            counter = 1

            while Article.objects.filter(slug=slug).exists():
                slug = f"{base_slug}-{counter}"

                counter += 1
            self.slug = slug

        super().save(*args, **kwargs)



    def increment_view_count(self):
        """Increment view count atomically"""
        self.view_count = models.F('view_count') + 1
        self.save(update_fields=['view_count'])
        self.refresh_from_db()
    
    @property
    def has_multiple_images(self):
        """Check if article has 2 images"""
        return bool(self.image_1 and self.image_2)
    
    @property
    def trending_score(self):
        """Calculate trending score based on recent views and recency"""
        
        
        # Days since publication
        days_old = (timezone.now() - self.published_date).days
        
        # Decay factor (newer = higher score)
        recency_factor = max(1, 30 - days_old) / 30
        
        # Trending score = views * recency
        return self.view_count * recency_factor
    

    



    

    


