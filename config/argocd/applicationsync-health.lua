-- Health of an ApplicationSync for Argo CD, under resource.customizations.health.argoproj.io_ApplicationSync in
-- argocd-cm. A run committed as a sync hook then holds its parent app's sync until it finishes, as a Job hook does.
local hs = {}
local phase = obj.status ~= nil and obj.status.phase or nil
if phase == "Succeeded" then
  hs.status = "Healthy"
  hs.message = "run succeeded"
elseif phase == "Failed" then
  hs.status = "Degraded"
  hs.message = obj.status.message or "run failed"
else
  hs.status = "Progressing"
  hs.message = (obj.status ~= nil and obj.status.message) or "run in progress"
end
return hs
