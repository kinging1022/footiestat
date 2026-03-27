from django.shortcuts import render, redirect
from django.contrib import messages
from django.core.mail import send_mail
from django.conf import settings


def about(request):
    return render(request, 'pages/about.html')


def contact(request):
    if request.method == 'POST':
        name = request.POST.get('name', '').strip()
        email = request.POST.get('email', '').strip()
        subject = request.POST.get('subject', '').strip()
        message = request.POST.get('message', '').strip()

        errors = {}
        if not name:
            errors['name'] = 'Your name is required.'
        if not email or '@' not in email:
            errors['email'] = 'A valid email address is required.'
        if not subject:
            errors['subject'] = 'Please provide a subject.'
        if not message or len(message) < 20:
            errors['message'] = 'Message must be at least 20 characters.'

        if not errors:
            try:
                send_mail(
                    subject=f'[FootieStat Contact] {subject}',
                    message=f'From: {name} <{email}>\n\n{message}',
                    from_email=settings.DEFAULT_FROM_EMAIL,
                    recipient_list=[settings.CONTACT_EMAIL],
                    fail_silently=False,
                )
                messages.success(request, 'Your message has been sent. We\'ll get back to you within 48 hours.')
                return redirect('pages:contact')
            except Exception:
                messages.error(request, 'There was a problem sending your message. Please try again or email us directly.')

        return render(request, 'pages/contact.html', {
            'errors': errors,
            'form_data': {'name': name, 'email': email, 'subject': subject, 'message': message},
        })

    return render(request, 'pages/contact.html')


def privacy(request):
    return render(request, 'pages/privacy.html')


def terms(request):
    return render(request, 'pages/terms.html')


def cookies(request):
    return render(request, 'pages/cookies.html')
