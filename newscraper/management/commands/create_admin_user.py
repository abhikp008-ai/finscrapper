from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from newscraper.models import UserProfile


class Command(BaseCommand):
    help = 'Create admin user with username "admin" and password "admin@1992x" if it doesn\'t exist'

    def handle(self, *args, **options):
        username = 'admin'
        email = 'admin@finscrap.com'
        password = 'admin@1992x'
        
        try:
            # Check if user already exists
            user = User.objects.get(username=username)
            self.stdout.write(
                self.style.WARNING(f'Admin user "{username}" already exists')
            )
            
            # Ensure user is superuser and staff
            if not user.is_superuser or not user.is_staff:
                user.is_superuser = True
                user.is_staff = True
                user.save()
                self.stdout.write(
                    self.style.SUCCESS(f'Updated "{username}" to have superuser and staff privileges')
                )
                
        except User.DoesNotExist:
            # Create the admin user
            user = User.objects.create_superuser(
                username=username,
                email=email,
                password=password
            )
            self.stdout.write(
                self.style.SUCCESS(f'Successfully created admin user "{username}"')
            )
        
        # Ensure UserProfile exists for admin user
        profile, created = UserProfile.objects.get_or_create(
            user=user,
            defaults={
                'can_monitor': True,
                'can_download': True,
            }
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS(f'Created UserProfile for "{username}"')
            )
        
        self.stdout.write(
            self.style.SUCCESS(f'Admin user setup complete. Username: "{username}", Password: "{password}"')
        )