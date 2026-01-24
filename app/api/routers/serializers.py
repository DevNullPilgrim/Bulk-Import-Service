from app.models.import_job import ImportJob


def job_to_dict(job: ImportJob) -> dict:
    return {
        'id': str(job.id),
        'status': job.status.value if hasattr(job.status, 'value') else str(job.status),
        'mode': job.mode.value if hasattr(job.mode, 'value') else str(job.mode),
        'filename': job.filename,
        'total_rows': job.total_rows,
        'processed_rows': job.processed_rows,
        'error': job.error,
        'created_at': job.created_at.isoformat() if getattr(job, 'created_at', None) else None,
    }
