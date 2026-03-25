# blog/forms.py

from django import forms
from .models import Article

class ArticleForm(forms.ModelForm):
    tags_input = forms.CharField(
        required=False,
        help_text="Enter tags separated by commas",
        widget=forms.TextInput(attrs={'placeholder': 'premier league, tactics, analysis'})
    )
    
    class Meta:
        model = Article
        fields = [
            'title', 'article_type', 'image_1', 'image_2', 
            'excerpt', 'content', 'read_time', 'is_published', 'is_featured'
        ]
        widgets = {
            'title': forms.TextInput(attrs={'placeholder': 'Enter article title'}),
            'excerpt': forms.Textarea(attrs={'rows': 3, 'placeholder': 'Brief summary (max 300 characters)'}),
            'content': forms.Textarea(attrs={'rows': 15, 'placeholder': 'Full article content (HTML supported)'}),
            'image_1': forms.URLInput(attrs={'placeholder': 'https://example.com/image1.jpg'}),
            'image_2': forms.URLInput(attrs={'placeholder': 'https://example.com/image2.jpg (optional)'}),
        }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Pre-fill tags_input from tags JSON field
        if self.instance and self.instance.tags:
            self.fields['tags_input'].initial = ', '.join(self.instance.tags)
    
    def clean_tags_input(self):
        """Convert comma-separated tags to list"""
        tags_input = self.cleaned_data.get('tags_input', '')
        if tags_input:
            return [tag.strip() for tag in tags_input.split(',') if tag.strip()]
        return []
    
    def save(self, commit=True):
        article = super().save(commit=False)
        article.tags = self.cleaned_data.get('tags_input', [])
        if commit:
            article.save()
        return article