from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader

def home(request):
    template = loader.get_template("main/home.html")
    context = {}
    if request.method == 'POST' and 'action_button' in request.POST:
        action = request.POST['action_button']
        action_name = action
        if "_" in action_name:
            action_name = action_name.replace("_", " ")
        # return user to required page
        return HttpResponseRedirect("{}/".format(action))
    return HttpResponse(template.render(context,request))